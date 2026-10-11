import json
import struct
import unittest
import zlib
from unittest import mock

from dmoj.packet import PacketManager


class FakeConnection:
    """A socket that records what is sent and can be made to fail."""

    def __init__(self) -> None:
        self.sent: list = []
        self.dead = False

    def sendall(self, data: bytes) -> None:
        if self.dead:
            raise ConnectionResetError('peer closed')
        size = struct.unpack('!I', data[:4])[0]
        self.sent.append(json.loads(zlib.decompress(data[4 : 4 + size])))

    def close(self) -> None:
        pass

    def shutdown(self, how: int) -> None:
        pass


def packet_manager(conn: FakeConnection) -> PacketManager:
    with mock.patch.object(PacketManager, '_do_reconnect'):
        manager = PacketManager('site', 9999, judge=mock.Mock(), name='judge', key='key')
    manager.conn = conn
    manager.judge.current_submission = mock.Mock(id=42)
    return manager


class PacketReplayTest(unittest.TestCase):
    def names(self, conn: FakeConnection) -> list:
        return [p['name'] for p in conn.sent]

    def test_submission_packets_are_sent_again_after_a_reconnect(self):
        first = FakeConnection()
        manager = packet_manager(first)
        manager.submission_acknowledged_packet(42)
        manager.begin_grading_packet(False)
        manager.test_case_status_packet(
            1,
            mock.Mock(
                result_flag=0,
                execution_time=0.1,
                points=1,
                total_points=1,
                max_memory=1,
                output='',
                extended_feedback='',
                feedback='',
                context_switches=(0, 0),
                runtime_version='',
                readable_codes=lambda: ['AC'],
            ),
        )
        manager._flush_testcase_queue()
        self.assertEqual(self.names(first), ['submission-acknowledged', 'grading-begin', 'test-case-status'])

        # The site drops the connection. A packet sent meanwhile is kept, not lost; the reconnection replays all.
        first.dead = True
        second = FakeConnection()

        def reconnect():
            manager.conn = second

        with mock.patch.object(manager, '_reconnect', side_effect=reconnect):
            manager.batch_begin_packet()
        self.assertEqual(
            self.names(second), ['submission-acknowledged', 'grading-begin', 'test-case-status', 'batch-begin']
        )
        self.assertEqual(second.sent[2]['cases'][0]['position'], 1)

        # Grading goes on, on the new connection, and finishing it forgets the story.
        manager.grading_end_packet()
        self.assertEqual(self.names(second)[-1], 'grading-end')
        self.assertEqual(manager._replay, [])

    def test_a_new_submission_starts_a_new_story(self):
        conn = FakeConnection()
        manager = packet_manager(conn)
        manager.submission_acknowledged_packet(41)
        manager.grading_end_packet()
        manager.submission_acknowledged_packet(42)
        manager.begin_grading_packet(True)
        self.assertEqual([p.get('submission-id') for p in manager._replay], [42, 42])

    def test_nothing_is_replayed_when_nothing_was_grading(self):
        first = FakeConnection()
        manager = packet_manager(first)
        manager.judge.current_submission = None
        first.dead = True
        second = FakeConnection()
        with mock.patch.object(manager, '_reconnect', side_effect=lambda: setattr(manager, 'conn', second)):
            manager.ping_packet(0.0)
        self.assertEqual(second.sent, [])

    def test_a_submission_finishing_during_the_outage_is_told_in_full_on_reconnect(self):
        first = FakeConnection()
        manager = packet_manager(first)
        manager.submission_acknowledged_packet(42)
        manager.begin_grading_packet(False)
        first.dead = True
        # Grading ends while the site is away: the first packet to fail triggers the reconnection, and the whole
        # story, up to and including the verdict, goes out on the new connection; afterwards nothing is kept.
        second = FakeConnection()
        with mock.patch.object(manager, '_reconnect', side_effect=lambda: setattr(manager, 'conn', second)):
            manager.grading_end_packet()
        self.assertEqual(self.names(second), ['submission-acknowledged', 'grading-begin', 'grading-end'])
        self.assertEqual(manager._replay, [])

    def test_a_verdict_sent_while_still_disconnected_waits_for_the_reconnect(self):
        # The reading thread is reconnecting (the connection is marked dead); the grading thread sends the verdict
        # meanwhile. It must be kept for the replay, not dropped.
        first = FakeConnection()
        manager = packet_manager(first)
        manager.submission_acknowledged_packet(42)
        manager.begin_grading_packet(False)
        manager._dead.add(first)
        manager.grading_end_packet()
        self.assertEqual(
            [p['name'] for p in manager._replay], ['submission-acknowledged', 'grading-begin', 'grading-end']
        )
        second = FakeConnection()
        manager.conn = second
        manager._dead.discard(first)
        with manager._lock:
            manager._resend_submission()
        self.assertEqual(self.names(second), ['submission-acknowledged', 'grading-begin', 'grading-end'])
        self.assertEqual(manager._replay, [])
