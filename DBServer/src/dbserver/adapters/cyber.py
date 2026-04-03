from __future__ import annotations

import importlib
import math
import time
import zlib
from dataclasses import dataclass
from queue import Empty, Queue
from threading import Event, Lock, Thread
from typing import Any, Callable, Iterable

from dbserver.domain.records import BoxRecord, FrameBatch


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(slots=True, frozen=True)
class CyberRuntimeBindings:
    module_name: str
    module: Any
    node_class: type[Any]

    def init(self) -> None:
        init_fn = getattr(self.module, "init", None)
        if not callable(init_fn):
            raise RuntimeError(f"cyber runtime module {self.module_name} does not expose init()")
        init_fn()

    def shutdown(self) -> None:
        shutdown_fn = getattr(self.module, "shutdown", None)
        if callable(shutdown_fn):
            shutdown_fn()

    def is_shutdown(self) -> bool:
        is_shutdown_fn = getattr(self.module, "is_shutdown", None)
        if callable(is_shutdown_fn):
            return bool(is_shutdown_fn())
        return False


def load_cyber_runtime(
    module_names: Iterable[str] = (
        "cyber.python.cyber_py3.cyber",
        "cyber_py3.cyber",
        "cyber",
    ),
) -> CyberRuntimeBindings:
    errors: list[str] = []
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:
            errors.append(f"{module_name}: {exc}")
            continue
        node_class = getattr(module, "Node", None)
        if node_class is None:
            errors.append(f"{module_name}: missing Node")
            continue
        return CyberRuntimeBindings(
            module_name=module_name,
            module=module,
            node_class=node_class,
        )
    raise ModuleNotFoundError(
        "unable to import a supported cyber runtime module; tried: " + "; ".join(errors)
    )


def _load_boxes_message_class() -> type[Any]:
    try:
        from dbserver.proto import inno_box_pb2
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "protobuf runtime is required for cyber ingest; install dbserver with the 'cyber' extra"
        ) from exc
    return inno_box_pb2.Boxes


def _has_field(message: Any, field_name: str) -> bool:
    has_field = getattr(message, "HasField", None)
    if not callable(has_field):
        return False
    try:
        return bool(has_field(field_name))
    except ValueError:
        return False


def _meters_to_mm(value: Any) -> int:
    numeric = float(value)
    if math.isnan(numeric) or math.isinf(numeric):
        return 0
    return int(round(numeric * 1000.0))


class CyberBoxesFrameDecoder:
    def __init__(self, boxes_message_class: type[Any] | None = None) -> None:
        self.boxes_message_class = boxes_message_class or _load_boxes_message_class()

    def decode(self, *, device_id: int, payload: Any) -> FrameBatch:
        message = self._coerce_message(payload)
        event_time_ms = self._resolve_event_time_ms(message)
        frame_id = self._resolve_frame_id(message)
        records = tuple(
            self._decode_box(
                device_id=device_id,
                frame_id=frame_id,
                event_time_ms=event_time_ms,
                box=box,
            )
            for box in message.box
        )
        return FrameBatch(
            device_id=device_id,
            frame_id=frame_id,
            event_time_ms=event_time_ms,
            records=records,
        )

    def _coerce_message(self, payload: Any) -> Any:
        if isinstance(payload, self.boxes_message_class):
            return payload
        if isinstance(payload, (bytes, bytearray, memoryview)):
            message = self.boxes_message_class()
            message.ParseFromString(bytes(payload))
            return message
        parse_from_string = getattr(payload, "SerializeToString", None)
        if callable(parse_from_string):
            serialized = payload.SerializeToString()
            message = self.boxes_message_class()
            message.ParseFromString(serialized)
            return message
        raise TypeError(f"unsupported cyber payload type: {type(payload)!r}")

    def _resolve_event_time_ms(self, message: Any) -> int:
        if _has_field(message, "lidar_timestamp") and int(message.lidar_timestamp) > 0:
            return int(message.lidar_timestamp) // 1_000_000
        if _has_field(message, "timestamp_ms") and int(message.timestamp_ms) > 0:
            return int(message.timestamp_ms)
        if _has_field(message, "timestamp") and int(message.timestamp) > 0:
            return int(message.timestamp) // 1_000_000
        if _has_field(message, "timestamp_sec") and float(message.timestamp_sec) > 0:
            return int(round(float(message.timestamp_sec) * 1000.0))
        raise ValueError(
            "protobuf Boxes payload is missing lidar_timestamp/timestamp_ms/timestamp/timestamp_sec"
        )

    def _resolve_frame_id(self, message: Any) -> int:
        if _has_field(message, "frame_id") and str(message.frame_id).strip():
            raw_frame_id = str(message.frame_id).strip()
            try:
                return int(raw_frame_id)
            except ValueError:
                if _has_field(message, "idx"):
                    return int(message.idx)
                if _has_field(message, "sequence_num"):
                    return int(message.sequence_num)
                return int(zlib.crc32(raw_frame_id.encode("utf-8")) & 0x7FFFFFFF)
        if _has_field(message, "idx"):
            return int(message.idx)
        if _has_field(message, "sequence_num"):
            return int(message.sequence_num)
        raise ValueError("protobuf Boxes payload is missing frame_id/idx/sequence_num")

    def _decode_box(
        self,
        *,
        device_id: int,
        frame_id: int,
        event_time_ms: int,
        box: Any,
    ) -> BoxRecord:
        speed_centi_kmh_100 = self._resolve_speed(box)
        lane_id = self._resolve_lane(box)
        return BoxRecord(
            trace_id=int(box.track_id),
            device_id=device_id,
            event_time_ms=event_time_ms,
            obj_type=int(box.object_type),
            position_x_mm=_meters_to_mm(box.position_x),
            position_y_mm=_meters_to_mm(box.position_y),
            position_z_mm=_meters_to_mm(box.position_z),
            length_mm=_meters_to_mm(box.length),
            width_mm=_meters_to_mm(box.width),
            height_mm=_meters_to_mm(box.height),
            speed_centi_kmh_100=speed_centi_kmh_100,
            spindle_centi_deg_100=int(box.spindle),
            lane_id=lane_id,
            frame_id=frame_id,
        )

    def _resolve_speed(self, box: Any) -> int:
        if _has_field(box, "speed"):
            return int(round(float(box.speed) * 3.6))
        components = []
        for field_name in ("speed_x", "speed_y", "speed_z"):
            if _has_field(box, field_name):
                components.append(float(getattr(box, field_name)))
        if not components:
            return 0
        magnitude = math.sqrt(sum(component * component for component in components))
        return int(round(magnitude * 360.0))

    def _resolve_lane(self, box: Any) -> int | None:
        if not _has_field(box, "lane"):
            return None
        lane_id = int(box.lane)
        if lane_id >= 999 or lane_id < 0:
            return None
        return lane_id


@dataclass(slots=True, frozen=True)
class CyberSubscriberSnapshot:
    enabled: bool
    node_name: str
    channels: tuple[str, ...]
    started_at_ms: int | None
    received_messages: int
    decoded_frames: int
    decoded_boxes: int
    decode_failures: int
    ingest_failures: int
    queue_size: int
    last_error: str | None
    cyber_runtime_module: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "node_name": self.node_name,
            "channels": list(self.channels),
            "started_at_ms": self.started_at_ms,
            "received_messages": self.received_messages,
            "decoded_frames": self.decoded_frames,
            "decoded_boxes": self.decoded_boxes,
            "decode_failures": self.decode_failures,
            "ingest_failures": self.ingest_failures,
            "queue_size": self.queue_size,
            "last_error": self.last_error,
            "cyber_runtime_module": self.cyber_runtime_module,
        }


class CyberBoxesSubscriber:
    def __init__(
        self,
        *,
        device_ids: Iterable[int],
        channel_template: str = "omnisense/test/{device_id}/boxes",
        node_name: str = "dbserver_cyber_ingest",
        queue_maxsize: int = 1024,
        decoder: CyberBoxesFrameDecoder | None = None,
        cyber_runtime: CyberRuntimeBindings | None = None,
    ) -> None:
        ordered_device_ids = tuple(dict.fromkeys(int(device_id) for device_id in device_ids))
        if not ordered_device_ids:
            raise ValueError("device_ids must not be empty")
        self.device_ids = ordered_device_ids
        self.channel_template = channel_template
        self.node_name = node_name
        self.queue_maxsize = max(int(queue_maxsize), 1)
        self.decoder = decoder or CyberBoxesFrameDecoder()
        self._cyber_runtime = cyber_runtime
        self._node: Any | None = None
        self._readers: list[Any] = []
        self._worker_thread: Thread | None = None
        self._spin_thread: Thread | None = None
        self._stop_event = Event()
        self._queue: Queue[tuple[str, FrameBatch]] = Queue(maxsize=self.queue_maxsize)
        self._lock = Lock()
        self._started_at_ms: int | None = None
        self._received_messages = 0
        self._decoded_frames = 0
        self._decoded_boxes = 0
        self._decode_failures = 0
        self._ingest_failures = 0
        self._last_error: str | None = None
        self._started = False

    def start(self, on_frame: Callable[[FrameBatch, str], None]) -> None:
        with self._lock:
            if self._started:
                return
            runtime = self._cyber_runtime or load_cyber_runtime()
            runtime.init()
            self._cyber_runtime = runtime
            self._node = runtime.node_class(self.node_name)
            for device_id in self.device_ids:
                channel = self._channel_name(device_id)
                reader = self._create_reader(channel=channel, device_id=device_id)
                self._readers.append(reader)
            self._stop_event.clear()
            self._worker_thread = Thread(
                target=self._run_worker,
                args=(on_frame,),
                daemon=True,
                name=f"{self.node_name}-worker",
            )
            self._worker_thread.start()
            self._spin_thread = Thread(
                target=self._run_spin,
                daemon=True,
                name=f"{self.node_name}-spin",
            )
            self._spin_thread.start()
            self._started_at_ms = _now_ms()
            self._started = True

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._stop_event.set()
            if self._cyber_runtime is not None:
                self._cyber_runtime.shutdown()
            worker_thread = self._worker_thread
            spin_thread = self._spin_thread
            self._started = False
        if worker_thread is not None:
            worker_thread.join(timeout=5.0)
        if spin_thread is not None:
            spin_thread.join(timeout=5.0)

    def snapshot(self) -> CyberSubscriberSnapshot:
        with self._lock:
            return CyberSubscriberSnapshot(
                enabled=self._started,
                node_name=self.node_name,
                channels=tuple(self._channel_name(device_id) for device_id in self.device_ids),
                started_at_ms=self._started_at_ms,
                received_messages=self._received_messages,
                decoded_frames=self._decoded_frames,
                decoded_boxes=self._decoded_boxes,
                decode_failures=self._decode_failures,
                ingest_failures=self._ingest_failures,
                queue_size=self._queue.qsize(),
                last_error=self._last_error,
                cyber_runtime_module=(
                    None
                    if self._cyber_runtime is None
                    else self._cyber_runtime.module_name
                ),
            )

    def _create_reader(self, *, channel: str, device_id: int) -> Any:
        if self._node is None:
            raise RuntimeError("cyber node is not initialized")

        def _callback(payload: Any) -> None:
            self._handle_payload(device_id=device_id, channel=channel, payload=payload)

        create_reader = getattr(self._node, "create_reader", None)
        if not callable(create_reader):
            raise RuntimeError("cyber node does not expose create_reader()")
        try:
            return create_reader(channel, self.decoder.boxes_message_class, _callback)
        except TypeError:
            return create_reader(channel, _callback)

    def _handle_payload(self, *, device_id: int, channel: str, payload: Any) -> None:
        try:
            frame = self.decoder.decode(device_id=device_id, payload=payload)
            self._queue.put((channel, frame), block=True)
            with self._lock:
                self._received_messages += 1
                self._decoded_frames += 1
                self._decoded_boxes += frame.box_count
                self._last_error = None
        except Exception as exc:
            with self._lock:
                self._decode_failures += 1
                self._last_error = str(exc)

    def _run_worker(self, on_frame: Callable[[FrameBatch, str], None]) -> None:
        while not self._stop_event.is_set():
            try:
                channel, frame = self._queue.get(timeout=0.1)
            except Empty:
                continue
            try:
                on_frame(frame, channel)
            except Exception as exc:
                with self._lock:
                    self._ingest_failures += 1
                    self._last_error = str(exc)
            finally:
                self._queue.task_done()

    def _run_spin(self) -> None:
        try:
            if self._node is not None and hasattr(self._node, "spin"):
                self._node.spin()
                return
            while not self._stop_event.is_set():
                if self._cyber_runtime is not None and self._cyber_runtime.is_shutdown():
                    return
                time.sleep(0.1)
        except Exception as exc:
            with self._lock:
                self._last_error = str(exc)

    def _channel_name(self, device_id: int) -> str:
        return self.channel_template.format(device_id=device_id, num=device_id)
