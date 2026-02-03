"""
Shared-memory ring buffer that allows a node leader to stage sequential
chunks for consumption by other ranks on the same node.
"""

import struct
import threading
import time
from typing import List, Optional

import numpy as np
from mpi4py import MPI

from node_fetcher import BufferRecord

HEADER_STRUCT = struct.Struct("<iiiiqiI")
HEADER_SIZE = HEADER_STRUCT.size

# Metadata indices
META_HEAD = 0        # number of produced chunks
META_CLAIMED = 1     # number of chunks handed out to consumers
META_TAIL = 2        # number of chunks fully acknowledged
META_CLOSED = 3      # boolean flag (0=open, 1=closed)
META_ACTIVE = 4      # active producers count
META_NUM_SLOTS = 5
META_MAX_PAYLOAD = 6
META_COUNT = 7


class SharedSlotManager(object):
    """Owns the shared-memory windows used for producer/consumer coordination."""

    def __init__(
        self,
        shm_comm: MPI.Comm,
        stream_names: List[str],
        max_payload: int,
        num_slots: int,
    ):
        self.shm_comm = shm_comm
        self.stream_names = list(stream_names)
        self.max_payload = max_payload
        self.num_slots = num_slots
        self.slot_size = HEADER_SIZE + max_payload
        self.node_rank = shm_comm.Get_rank()
        self.is_leader = self.node_rank == 0
        # Allocate metadata window collectively.
        meta_bytes = META_COUNT * np.dtype(np.int64).itemsize if self.is_leader else 0
        self.meta_win = MPI.Win.Allocate_shared(
            meta_bytes,
            np.dtype(np.int64).itemsize if meta_bytes else 1,
            comm=shm_comm,
        )
        base, _ = self.meta_win.Shared_query(0)
        self.meta = np.ndarray(
            buffer=base,
            dtype=np.int64,
            shape=(META_COUNT,),
        )
        if self.is_leader:
            self.meta[:] = 0
            self.meta[META_NUM_SLOTS] = num_slots
            self.meta[META_MAX_PAYLOAD] = max_payload
        self.meta_win.Fence()
        # Allocate payload window.
        data_bytes = self.slot_size * num_slots if self.is_leader else 0
        self.data_win = MPI.Win.Allocate_shared(
            data_bytes,
            1 if data_bytes else 1,
            comm=shm_comm,
        )
        data_base, _ = self.data_win.Shared_query(0)
        self.data = np.ndarray(buffer=data_base, dtype=np.uint8, shape=(self.slot_size * num_slots,))
        if self.is_leader:
            self.data[:] = 0
        self.data_win.Fence()

    def create_producer(self) -> "SharedSlotProducer":
        if not self.is_leader:
            raise RuntimeError("Only node leaders may create producers")
        return SharedSlotProducer(self)

    def create_consumer(self) -> "SharedSlotConsumer":
        return SharedSlotConsumer(self)

    def close(self) -> None:
        self.meta_win.Free()
        self.data_win.Free()


class SharedSlotProducer(object):
    """Implements the producer-facing RingBuffer API for NodeFetcher."""

    def __init__(self, manager: SharedSlotManager):
        self.manager = manager
        self.stream_to_idx = {name: idx for idx, name in enumerate(manager.stream_names)}
        self._producer_lock = threading.Lock()

    def set_producer_count(self, count: int) -> None:
        with self._producer_lock:
            self.manager.meta[META_ACTIVE] = count
            if count == 0:
                self.manager.meta[META_CLOSED] = 1
            self.manager.meta_win.Sync()

    def produce(self, record: BufferRecord) -> None:
        payload = record.payload
        payload_len = len(payload)
        if payload_len > self.manager.max_payload:
            raise ValueError(f"Payload {payload_len} exceeds max {self.manager.max_payload}")
        stream_idx = self.stream_to_idx[record.stream]
        while True:
            head = int(self.manager.meta[META_HEAD])
            tail = int(self.manager.meta[META_TAIL])
            if head - tail < self.manager.num_slots:
                slot_index = head % self.manager.num_slots
                break
            time.sleep(0.001)
        with self._producer_lock:
            head = int(self.manager.meta[META_HEAD])
            tail = int(self.manager.meta[META_TAIL])
            while head - tail >= self.manager.num_slots:
                time.sleep(0.001)
                head = int(self.manager.meta[META_HEAD])
                tail = int(self.manager.meta[META_TAIL])
            slot_index = head % self.manager.num_slots
            base = slot_index * self.manager.slot_size
            # Copy payload
            start = base + HEADER_SIZE
            end = start + payload_len
            self.manager.data[start:end] = np.frombuffer(payload, dtype=np.uint8, count=payload_len)
            # Zero the remainder for cleanliness
            if payload_len < self.manager.max_payload:
                clear_start = end
                clear_end = base + HEADER_SIZE + self.manager.max_payload
                self.manager.data[clear_start:clear_end] = 0
            header_bytes = HEADER_STRUCT.pack(
                stream_idx,
                record.window_index,
                record.start_event,
                record.end_event,
                record.offset,
                payload_len,
                1,
            )
            self.manager.data[base : base + HEADER_SIZE] = np.frombuffer(header_bytes, dtype=np.uint8)
            self.manager.data_win.Sync()
            self.manager.meta[META_HEAD] = head + 1
            self.manager.meta_win.Sync()

    def producer_done(self) -> None:
        with self._producer_lock:
            active = int(self.manager.meta[META_ACTIVE])
            active -= 1
            self.manager.meta[META_ACTIVE] = active
            if active <= 0:
                self.manager.meta[META_CLOSED] = 1
            self.manager.meta_win.Sync()

    def close(self) -> None:
        # Provided for compatibility with NodeFetcher lifecycle; no-op.
        pass


class SharedSlotConsumer(object):
    """Provides consumption API for non-leader ranks."""

    def __init__(self, manager: SharedSlotManager):
        self.manager = manager
        self.stream_names = manager.stream_names

    def _claim_record_id(self) -> Optional[int]:
        while True:
            self.manager.meta_win.Lock(0, MPI.LOCK_EXCLUSIVE)
            head = int(self.manager.meta[META_HEAD])
            claimed = int(self.manager.meta[META_CLAIMED])
            closed = int(self.manager.meta[META_CLOSED])
            if claimed < head:
                record_id = claimed
                self.manager.meta[META_CLAIMED] = claimed + 1
                self.manager.meta_win.Unlock(0)
                return record_id
            if closed:
                self.manager.meta_win.Unlock(0)
                return None
            self.manager.meta_win.Unlock(0)
            time.sleep(0.001)

    def _ack_record(self, record_id: int) -> None:
        while True:
            self.manager.meta_win.Lock(0, MPI.LOCK_EXCLUSIVE)
            tail = int(self.manager.meta[META_TAIL])
            if tail == record_id:
                self.manager.meta[META_TAIL] = tail + 1
                self.manager.meta_win.Unlock(0)
                return
            self.manager.meta_win.Unlock(0)
            time.sleep(0.001)

    def consume_next(self) -> Optional[BufferRecord]:
        record_id = self._claim_record_id()
        if record_id is None:
            return None
        slot_index = record_id % self.manager.num_slots
        base = slot_index * self.manager.slot_size
        header_bytes = bytes(self.manager.data[base : base + HEADER_SIZE])
        (
            stream_idx,
            window_idx,
            start_event,
            end_event,
            offset,
            payload_len,
            _flags,
        ) = HEADER_STRUCT.unpack(header_bytes)
        payload_start = base + HEADER_SIZE
        payload_end = payload_start + payload_len
        payload = bytes(self.manager.data[payload_start:payload_end])
        self._ack_record(record_id)
        stream_name = self.stream_names[stream_idx]
        return BufferRecord(
            stream=stream_name,
            window_index=window_idx,
            start_event=start_event,
            end_event=end_event,
            offset=offset,
            length=payload_len,
            payload=payload,
        )
