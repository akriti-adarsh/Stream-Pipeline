"""Transactional ride sessionizer: rides.events -> rides.sessions, exactly once.

The exactly-once mechanics, end to end:

1. A stable ``transactional.id`` makes ``init_transactions()`` fence any zombie
   predecessor at its next commit attempt.
2. Each batch is processed, journaled to the offset-tagged SQLite store
   (:mod:`processors.state_store`), and only then wrapped in a Kafka
   transaction: produce the session records, ``send_offsets_to_transaction``
   with the consumer group metadata, ``commit_transaction``. Output records
   and input offsets commit or abort as one unit, so a session can never be
   published for input the group did not consume, nor consumed input lose its
   session.
3. The SQLite store commits BEFORE the Kafka transaction, which opens the
   two-store gap: a crash between the two commits leaves state the transaction
   never confirmed. The startup path closes it structurally: on assignment the
   Kafka-committed offset is read first, then ``rollback_from`` erases every
   state row tagged at or beyond it, and the batch simply replays. The
   ``crash_after_state_commit`` hook exists so a test can SIGKILL exactly
   inside that window and prove the recovery.
4. Consumers of rides.sessions must read with isolation.level=read_committed
   (the default in :func:`common.kafka.consumer_config`), otherwise aborted
   transactions would leak downstream and void all of the above.

Rides with no terminal event are closed as ``abandoned`` when the per-partition
event-time watermark (max event_ts seen) passes ``session_timeout_ms`` beyond
their last activity. Undeserialisable messages are handed to the poison
handler; the DLQ milestone plugs the envelope producer into it.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.serialization import MessageField, SerializationContext

from common import topics
from common.kafka import consumer_config, producer_config
from common.logging import configure_logging, with_ctx
from common.schemas import (
    make_registry_client,
    ride_event_deserializer,
    ride_session_serializer,
)
from common.times import to_epoch_ms
from common.topics import dlq_topic
from dlq.envelope import Envelope, build_envelope, envelope_bytes
from processors.session_builder import (
    SESSION_TIMEOUT_MS_DEFAULT,
    RideAccumulator,
    build_session,
    timed_out,
)
from processors.state_store import CloseRow, JournalRow, StateStore

PoisonHandler = Callable[[Any, Exception], None]


@dataclass
class SessionizerConfig:
    bootstrap: str = "localhost:19092"
    registry_url: str = "http://localhost:18081"
    group_id: str = "sessionizer"
    transactional_id: str = "sessionizer-tx-1"
    state_path: str = "state/sessionizer.db"
    session_timeout_ms: int = SESSION_TIMEOUT_MS_DEFAULT
    batch_max: int = 500
    poll_timeout_sec: float = 1.0
    crash_after_state_commit: bool = False

    @classmethod
    def from_env(cls) -> SessionizerConfig:
        return cls(
            bootstrap=os.environ.get("KAFKA_BOOTSTRAP", cls.bootstrap),
            registry_url=os.environ.get("SCHEMA_REGISTRY_URL", cls.registry_url),
            state_path=os.environ.get("STATE_PATH", cls.state_path),
            session_timeout_ms=int(
                os.environ.get("SESSION_TIMEOUT_MS", str(SESSION_TIMEOUT_MS_DEFAULT))
            ),
            crash_after_state_commit=os.environ.get("CRASH_AFTER_STATE_COMMIT", "") == "1",
        )


@dataclass
class _PartitionState:
    watermark_ms: int = 0
    max_offset_in_batch: int = -1


class Sessionizer:
    def __init__(
        self,
        cfg: SessionizerConfig,
        *,
        consumer: Any | None = None,
        producer: Any | None = None,
        deserialize: Callable[[str, bytes], dict[str, Any]] | None = None,
        serialize_session: Callable[[str, dict[str, Any]], bytes] | None = None,
        store: StateStore | None = None,
        on_poison: PoisonHandler | None = None,
    ) -> None:
        self._cfg = cfg
        self._log = configure_logging("sessionizer")
        self._store = store if store is not None else StateStore(cfg.state_path)
        self._consumer = (
            consumer
            if consumer is not None
            else Consumer(consumer_config(cfg.bootstrap, cfg.group_id))
        )
        self._producer = (
            producer
            if producer is not None
            else Producer(producer_config(cfg.bootstrap, transactional_id=cfg.transactional_id))
        )
        if deserialize is None or serialize_session is None:
            client = make_registry_client(cfg.registry_url)
            avro_deser = ride_event_deserializer(client)
            avro_ser = ride_session_serializer(client)

            def _deserialize(topic: str, payload: bytes) -> dict[str, Any]:
                value = avro_deser(payload, SerializationContext(topic, MessageField.VALUE))
                assert isinstance(value, dict)
                return value

            def _serialize(topic: str, value: dict[str, Any]) -> bytes:
                data = avro_ser(value, SerializationContext(topic, MessageField.VALUE))
                assert isinstance(data, bytes)
                return data

            deserialize = deserialize if deserialize is not None else _deserialize
            serialize_session = serialize_session if serialize_session is not None else _serialize
        self._deserialize = deserialize
        self._serialize_session = serialize_session
        self._on_poison: PoisonHandler = on_poison if on_poison is not None else self._log_poison
        self._accs: dict[str, RideAccumulator] = {}
        self._acc_partition: dict[str, int] = {}
        self._partitions: dict[int, _PartitionState] = {}
        self.sessions_emitted = 0
        self.poison_seen = 0

    # ---------------------------------------------------------------- startup

    def _log_poison(self, msg: Any, error: Exception) -> None:
        self._log.warning(
            "poison message skipped (DLQ producer attaches at the DLQ milestone)",
            extra=with_ctx(
                topic=msg.topic(), partition=msg.partition(), offset=msg.offset(), error=str(error)
            ),
        )

    def on_assign(self, consumer: Any, partitions: list[Any]) -> None:
        """Recovery: Kafka's transactionally-committed offsets decide; local
        state tagged at or beyond them is erased before we consume a byte."""
        committed = consumer.committed(partitions, timeout=15)
        for tp in committed:
            resume = tp.offset if tp.offset >= 0 else 0
            rolled_j, rolled_c = self._store.rollback_from(tp.partition, resume)
            purged = self._store.purge_confirmed(tp.partition, resume)
            open_rides = self._store.open_rides(tp.partition)
            for ride_id, events in open_rides.items():
                acc = RideAccumulator(ride_id)
                for event in events:
                    acc.add(event)
                self._accs[ride_id] = acc
                self._acc_partition[ride_id] = tp.partition
            watermark = self._store.max_journal_ts(tp.partition) or 0
            self._partitions[tp.partition] = _PartitionState(watermark_ms=watermark)
            self._log.info(
                "partition assigned",
                extra=with_ctx(
                    partition=tp.partition,
                    resume_offset=resume,
                    rolled_back_journal=rolled_j,
                    rolled_back_closes=rolled_c,
                    purged_journal=purged,
                    open_rides=len(open_rides),
                ),
            )

    def on_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        for tp in partitions:
            for ride_id in [r for r, p in self._acc_partition.items() if p == tp.partition]:
                self._accs.pop(ride_id, None)
                self._acc_partition.pop(ride_id, None)
            self._partitions.pop(tp.partition, None)

    # ------------------------------------------------------------------- loop

    def run(self, max_batches: int | None = None, idle_timeout_sec: float | None = None) -> None:
        """Consume forever, or until max_batches non-empty batches, or until the
        topic has been quiet for idle_timeout_sec (bounded runs in tests)."""
        self._producer.init_transactions()
        self._consumer.subscribe(
            [topics.RIDES_EVENTS], on_assign=self.on_assign, on_revoke=self.on_revoke
        )
        batches = 0
        last_data = time.monotonic()
        self._log.info("sessionizer running", extra=with_ctx(group=self._cfg.group_id))
        try:
            while max_batches is None or batches < max_batches:
                msgs = self._consumer.consume(self._cfg.batch_max, self._cfg.poll_timeout_sec)
                if not msgs:
                    if idle_timeout_sec is not None and (
                        time.monotonic() - last_data > idle_timeout_sec
                    ):
                        break
                    continue
                last_data = time.monotonic()
                self.process_batch(msgs)
                batches += 1
        finally:
            self._consumer.close()

    # ------------------------------------------------------------------ batch

    def process_batch(self, msgs: list[Any]) -> list[dict[str, Any]]:
        journal: list[JournalRow] = []
        batch_state: dict[int, _PartitionState] = {}
        poisons: list[Envelope] = []

        for msg in msgs:
            if msg.error() is not None:
                error = msg.error()
                if error.code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"consumer error: {error}")
            partition = msg.partition()
            pstate = self._partitions.setdefault(partition, _PartitionState())
            bstate = batch_state.setdefault(partition, _PartitionState(max_offset_in_batch=-1))
            bstate.max_offset_in_batch = max(bstate.max_offset_in_batch, msg.offset())
            try:
                value = self._deserialize(msg.topic(), msg.value())
            except Exception as error:  # poison messages take the DLQ path
                self.poison_seen += 1
                poisons.append(build_envelope(msg, error, self._cfg.group_id))
                self._on_poison(msg, error)
                continue
            value = {**value, "event_ts": to_epoch_ms(value["event_ts"])}
            ride_id = str(value["ride_id"])
            pstate.watermark_ms = max(pstate.watermark_ms, int(value["event_ts"]))
            if self._store.is_closed(ride_id):
                continue  # late event for an already-closed ride; the session is final
            journal.append(JournalRow(partition, msg.offset(), ride_id, value))
            acc = self._accs.get(ride_id)
            if acc is None:
                acc = RideAccumulator(ride_id)
                self._accs[ride_id] = acc
                self._acc_partition[ride_id] = partition
            acc.add(value)

        closes, sessions = self._collect_closes(batch_state)
        self._store.append_batch(journal, closes)

        if self._cfg.crash_after_state_commit and closes:
            # Test hook: die INSIDE the two-store gap, after the SQLite commit
            # and before the Kafka transaction. Recovery must erase the closes.
            self._log.warning("crash hook firing inside the two-store gap")
            os._exit(137)

        self._transact(sessions, batch_state, poisons)
        for close in closes:
            self._accs.pop(close.ride_id, None)
            self._acc_partition.pop(close.ride_id, None)
        return [dict(s) for s in sessions]

    def _collect_closes(
        self, batch_state: dict[int, _PartitionState]
    ) -> tuple[list[CloseRow], list[dict[str, Any]]]:
        closes: list[CloseRow] = []
        sessions: list[dict[str, Any]] = []
        for ride_id, acc in list(self._accs.items()):
            partition = self._acc_partition[ride_id]
            if partition not in batch_state:
                continue  # only partitions with new input can commit anything
            tag_offset = batch_state[partition].max_offset_in_batch
            pstate = self._partitions[partition]
            if acc.terminal is not None:
                session = build_session(acc)
            elif timed_out(acc, pstate.watermark_ms, self._cfg.session_timeout_ms):
                session = build_session(acc, closed_by_timeout=True)
            else:
                continue
            closes.append(CloseRow(ride_id, partition, tag_offset, session))
            sessions.append(session)
        return closes, sessions

    def _transact(
        self,
        sessions: list[dict[str, Any]],
        batch_state: dict[int, _PartitionState],
        poisons: list[Envelope] | None = None,
    ) -> None:
        offsets = [
            TopicPartition(topics.RIDES_EVENTS, partition, state.max_offset_in_batch + 1)
            for partition, state in batch_state.items()
            if state.max_offset_in_batch >= 0
        ]
        try:
            self._producer.begin_transaction()
            for session in sessions:
                payload = self._serialize_session(topics.RIDES_SESSIONS, session)
                self._producer.produce(
                    topic=topics.RIDES_SESSIONS,
                    key=str(session["ride_id"]).encode(),
                    value=payload,
                )
            for envelope in poisons or []:
                # Dead letters ride in the same transaction: a DLQ record exists
                # if and only if the offsets that produced it were committed.
                self._producer.produce(
                    topic=dlq_topic(envelope.source_topic),
                    key=envelope.key,
                    value=envelope_bytes(envelope),
                )
            self._producer.send_offsets_to_transaction(
                offsets, self._consumer.consumer_group_metadata()
            )
            self._producer.commit_transaction()
            self.sessions_emitted += len(sessions)
        except Exception:
            # Abort, erase the batch's local state, and let the supervisor
            # restart us: recovery replays the batch from the committed offset.
            self._producer.abort_transaction()
            for partition, state in batch_state.items():
                if state.max_offset_in_batch >= 0:
                    committed = self._consumer.committed(
                        [TopicPartition(topics.RIDES_EVENTS, partition)], timeout=15
                    )[0]
                    resume = committed.offset if committed.offset >= 0 else 0
                    self._store.rollback_from(partition, resume)
            raise


def main() -> int:
    cfg = SessionizerConfig.from_env()
    Sessionizer(cfg).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
