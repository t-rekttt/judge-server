import unittest
from types import SimpleNamespace
from unittest import mock

from dmoj.remote.microvm_backend import MicroVMBackend, MicroVMError, judge_tag


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = str(body)

    def json(self):
        return self._body


def config(**overrides):
    values = dict(
        api_url='https://api.example',
        api_token='api-token',
        access_token=None,
        region='mkc1',
        image='img:1',
        checkpoint_id=None,
        checkpoint_runner_token=None,
        cpus=2,
        memory_mb=4096,
        vpc_uuid=None,
        endpoint_scheme='https',
        ready_timeout=5,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def microvm(state, hostname='', status='PENDING', **extra):
    return {
        'microvm': {
            'id': 'vm-1',
            'state': state,
            'urls': [{'hostname': hostname, 'port': 8080, 'default': True, 'status': status}],
            **extra,
        }
    }


class MicroVMBackendTest(unittest.TestCase):
    def setUp(self):
        self.sleep = mock.patch('dmoj.remote.microvm_backend.time.sleep').start()
        self.healthy = mock.patch('dmoj.remote.microvm_backend.wait_until_healthy').start()
        self.addCleanup(mock.patch.stopall)

    def backend(self, responses, **overrides):
        backend = MicroVMBackend(config(**overrides), 'judge 1')
        backend._http = mock.Mock()
        backend._http.request.side_effect = responses
        return backend

    def test_create_from_image_waits_for_endpoint(self):
        backend = self.backend(
            [
                FakeResponse(201, microvm('creating')),
                FakeResponse(200, microvm('running')),  # running but endpoint not provisioned yet
                FakeResponse(200, microvm('running', 'vm-1.mkc1.microvm.example', 'ACTIVE')),
            ]
        )
        vm = backend.create_vm('42-1')
        self.assertEqual(vm.url, 'https://vm-1.mkc1.microvm.example')
        self.assertEqual(vm.headers, {'Authorization': 'Bearer api-token'})
        method, url = backend._http.request.call_args_list[0].args
        request = backend._http.request.call_args_list[0].kwargs['json']
        self.assertEqual((method, url), ('POST', 'https://api.example/v2/microvms'))
        self.assertEqual(request['source'], {'oci_ref': 'img:1'})
        self.assertEqual(request['size'], {'cpu': 2, 'memory': 4096})
        self.assertEqual(request['environment'], {'JUDGE_MODE': 'runner', 'RUNNER_TOKEN': vm.token})
        self.assertIn(judge_tag('judge 1'), request['tags'])
        self.assertRegex(request['name'], r'^[a-z0-9-]{1,63}$')

    def test_create_from_checkpoint_sends_no_inherited_fields(self):
        backend = self.backend(
            [FakeResponse(201, microvm('running', 'h', 'ACTIVE'))],
            image=None,
            checkpoint_id='ck',
            checkpoint_runner_token='shared',
        )
        vm = backend.create_vm('1')
        request = backend._http.request.call_args.kwargs['json']
        self.assertEqual(request['source'], {'checkpoint_id': 'ck'})
        for inherited in ('region', 'size', 'environment'):
            self.assertNotIn(inherited, request)
        self.assertEqual(vm.token, 'shared')

    def test_failed_vm_is_deleted(self):
        backend = self.backend(
            [
                FakeResponse(201, microvm('creating')),
                FakeResponse(200, microvm('failed', failure_reason='image pull failed')),
                FakeResponse(204),
            ]
        )
        with self.assertRaisesRegex(MicroVMError, 'image pull failed'):
            backend.create_vm('1')
        self.assertEqual(backend._http.request.call_args.args, ('DELETE', 'https://api.example/v2/microvms/vm-1'))

    def test_unhealthy_runner_is_deleted(self):
        self.healthy.side_effect = TimeoutError('no answer')
        backend = self.backend([FakeResponse(201, microvm('running', 'h', 'ACTIVE')), FakeResponse(204)])
        with self.assertRaises(TimeoutError):
            backend.create_vm('1')
        self.assertEqual(backend._http.request.call_args.args[0], 'DELETE')

    def test_rate_limits_and_server_errors_are_retried(self):
        backend = self.backend(
            [
                FakeResponse(429, {}, {'ratelimit-reset': 'x'}),
                FakeResponse(503, {}),
                FakeResponse(201, microvm('running', 'h', 'ACTIVE')),
            ]
        )
        backend.create_vm('1')
        self.assertEqual(backend._http.request.call_count, 3)

    def test_client_errors_are_not_retried(self):
        backend = self.backend([FakeResponse(400, {'code': 'BAD_REQUEST', 'message': 'invalid size'})])
        with self.assertRaisesRegex(MicroVMError, '400'):
            backend.create_vm('1')
        self.assertEqual(backend._http.request.call_count, 1)

    def test_deleting_a_vm_that_is_gone_is_fine(self):
        backend = self.backend([FakeResponse(404, {'code': 'NOT_FOUND'})])
        backend._delete('vm-1')

    def test_orphans_are_listed_by_tag_across_pages(self):
        backend = self.backend(
            [
                FakeResponse(200, {'microvms': [{'id': 'a'}], 'links': {'pages': {'next': '?page=2'}}}),
                FakeResponse(200, {'microvms': [{'id': 'b'}], 'links': {}}),
                FakeResponse(204),
                FakeResponse(204),
            ]
        )
        backend.delete_orphans()
        calls = backend._http.request.call_args_list
        self.assertEqual(calls[0].kwargs['params']['tag_name'], judge_tag('judge 1'))
        self.assertEqual(
            [c.args[1] for c in calls[2:]], ['https://api.example/v2/microvms/a', 'https://api.example/v2/microvms/b']
        )

    def test_config_must_name_one_source(self):
        with self.assertRaises(ValueError):
            MicroVMBackend(config(checkpoint_id='ck'), 'j')
        with self.assertRaises(ValueError):
            MicroVMBackend(config(image=None, checkpoint_id='ck'), 'j')
