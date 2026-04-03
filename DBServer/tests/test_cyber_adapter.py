from __future__ import annotations

import time
import unittest

from dbserver.adapters.cyber import (
    CyberBoxesFrameDecoder,
    CyberBoxesSubscriber,
    CyberRuntimeBindings,
)
from dbserver.proto import inno_box_pb2


def _wait_until(predicate, *, timeout_seconds: float = 5.0, step_seconds: float = 0.05) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(step_seconds)
    return False


class _FakeRuntimeModule:
    initialized = False
    shutdown_called = False

    @classmethod
    def reset(cls) -> None:
        cls.initialized = False
        cls.shutdown_called = False
        _FakeNode.last_created = None

    @classmethod
    def init(cls) -> None:
        cls.initialized = True

    @classmethod
    def shutdown(cls) -> None:
        cls.shutdown_called = True

    @classmethod
    def is_shutdown(cls) -> bool:
        return cls.shutdown_called


class _FakeNode:
    last_created: "_FakeNode | None" = None

    def __init__(self, name: str) -> None:
        self.name = name
        self.readers: dict[str, object] = {}
        _FakeNode.last_created = self

    def create_reader(self, channel: str, message_class: object, callback: object) -> object:
        self.readers[channel] = callback
        return object()

    def spin(self) -> None:
        while not _FakeRuntimeModule.shutdown_called:
            time.sleep(0.01)


class CyberAdapterTest(unittest.TestCase):
    def test_decoder_maps_boxes_proto_to_frame_batch(self) -> None:
        message = inno_box_pb2.Boxes()
        message.lidar_timestamp = 1711922400100123456
        message.timestamp_ms = 1711922400100
        message.frame_id = "frame-1001"
        message.idx = 1001

        box = message.box.add()
        box.track_id = 42
        box.object_type = 3
        box.position_x = 1.2
        box.position_y = -3.4
        box.position_z = 0.5
        box.length = 4.5
        box.width = 1.8
        box.height = 1.6
        box.speed = 100
        box.spindle = 9050
        box.lane = 999

        decoder = CyberBoxesFrameDecoder(boxes_message_class=inno_box_pb2.Boxes)
        frame = decoder.decode(device_id=2, payload=message)

        self.assertEqual(frame.device_id, 2)
        self.assertEqual(frame.frame_id, 1001)
        self.assertEqual(frame.event_time_ms, 1711922400100)
        self.assertEqual(frame.box_count, 1)
        record = frame.records[0]
        self.assertEqual(record.trace_id, 42)
        self.assertEqual(record.event_time_ms, 1711922400100)
        self.assertEqual(record.obj_type, 3)
        self.assertEqual(record.position_x_mm, 1200)
        self.assertEqual(record.position_y_mm, -3400)
        self.assertEqual(record.position_z_mm, 500)
        self.assertEqual(record.length_mm, 4500)
        self.assertEqual(record.width_mm, 1800)
        self.assertEqual(record.height_mm, 1600)
        self.assertEqual(record.speed_centi_kmh_100, 360)
        self.assertEqual(record.spindle_centi_deg_100, 9050)
        self.assertIsNone(record.lane_id)

    def test_subscriber_receives_proto_and_invokes_callback(self) -> None:
        _FakeRuntimeModule.reset()
        runtime = CyberRuntimeBindings(
            module_name="fake",
            module=_FakeRuntimeModule,
            node_class=_FakeNode,
        )
        subscriber = CyberBoxesSubscriber(
            device_ids=(3,),
            channel_template="omnisense/test/{num}/boxes",
            node_name="test_cyber_ingest",
            queue_maxsize=8,
            decoder=CyberBoxesFrameDecoder(boxes_message_class=inno_box_pb2.Boxes),
            cyber_runtime=runtime,
        )
        received: list[tuple[str, int, int]] = []

        try:
            subscriber.start(
                lambda frame, channel: received.append((channel, frame.device_id, frame.frame_id))
            )
            self.assertTrue(_FakeRuntimeModule.initialized)
            self.assertIsNotNone(_FakeNode.last_created)

            message = inno_box_pb2.Boxes()
            message.lidar_timestamp = 1711922400100000000
            message.timestamp = 1711922400100000000
            message.frame_id = "2002"
            box = message.box.add()
            box.track_id = 9
            box.object_type = 1
            box.position_x = 0.1
            box.position_y = 0.2
            box.position_z = 0.3
            box.length = 4.0
            box.width = 1.5
            box.height = 1.7
            box.speed_x = 1.0
            box.speed_y = 2.0
            box.speed_z = 0.0
            box.spindle = 1800
            box.lane = 4

            callback = _FakeNode.last_created.readers["omnisense/test/3/boxes"]
            callback(message.SerializeToString())

            self.assertTrue(_wait_until(lambda: len(received) == 1))
            self.assertEqual(received[0], ("omnisense/test/3/boxes", 3, 2002))

            snapshot = subscriber.snapshot()
            self.assertEqual(snapshot.received_messages, 1)
            self.assertEqual(snapshot.decoded_frames, 1)
            self.assertEqual(snapshot.decoded_boxes, 1)
            self.assertIsNone(snapshot.last_error)
        finally:
            subscriber.stop()
        self.assertTrue(_FakeRuntimeModule.shutdown_called)

    def test_decoder_prefers_lidar_timestamp_over_other_time_fields(self) -> None:
        message = inno_box_pb2.Boxes()
        message.lidar_timestamp = 1711922400999123456
        message.timestamp_ms = 1
        message.timestamp = 2
        message.timestamp_sec = 3.0
        message.frame_id = "3003"

        box = message.box.add()
        box.track_id = 1
        box.object_type = 1
        box.position_x = 0.0
        box.position_y = 0.0
        box.position_z = 0.0
        box.length = 1.0
        box.width = 1.0
        box.height = 1.0
        box.spindle = 0

        decoder = CyberBoxesFrameDecoder(boxes_message_class=inno_box_pb2.Boxes)
        frame = decoder.decode(device_id=1, payload=message)

        self.assertEqual(frame.event_time_ms, 1711922400999)
        self.assertEqual(frame.records[0].event_time_ms, 1711922400999)


if __name__ == "__main__":
    unittest.main()
