"""采集编排：观察文件、推进基线、复核歧义、幂等发布。"""

from dataclasses import dataclass, field
from pathlib import Path

from .config import SupplierConfig
from .diff import ChangeEvent, DiffResult, diff_snapshots
from .ingestion import ingest_bytes
from .models import Evidence, Snapshot
from .store import JsonStore


@dataclass
class Observation:
    digest: str
    source_name: str
    generation: int
    status: str  # first_baseline | advanced | late | rejected
    reason: str | None
    errors: tuple[str, ...]
    row_count: int
    event_ids: tuple[str, ...] = field(default_factory=tuple)
    ambiguity_ids: tuple[str, ...] = field(default_factory=tuple)
    baseline_digest_after: str | None = None
    baseline_generation_after: int | None = None

    @classmethod
    def from_record(cls, record: dict) -> 'Observation':
        return cls(
            digest=record['digest'],
            source_name=record['source_name'],
            generation=record['generation'],
            status=record['status'],
            reason=record.get('reason'),
            errors=tuple(record.get('errors', ())),
            row_count=record.get('row_count', 0),
            event_ids=tuple(record.get('event_ids', ())),
            ambiguity_ids=tuple(record.get('ambiguity_ids', ())),
            baseline_digest_after=record.get('baseline_digest_after'),
            baseline_generation_after=record.get('baseline_generation_after'),
        )

    def to_record(self) -> dict:
        return {
            'digest': self.digest,
            'source_name': self.source_name,
            'generation': self.generation,
            'status': self.status,
            'reason': self.reason,
            'errors': list(self.errors),
            'row_count': self.row_count,
            'event_ids': list(self.event_ids),
            'ambiguity_ids': list(self.ambiguity_ids),
            'baseline_digest_after': self.baseline_digest_after,
            'baseline_generation_after': self.baseline_generation_after,
        }


class CollectionEngine:
    def __init__(self, config: SupplierConfig, base_dir, sink=None):
        self.config = config
        self.store = JsonStore(base_dir, config.supplier)
        self.sink = sink

    # ---- 文件观察 ------------------------------------------------------
    def observe_bytes(
        self,
        content: bytes,
        *,
        generation: int,
        source_name: str,
        declared_rows: int | None = None,
        declared_complete: bool = True,
    ) -> Observation:
        result = ingest_bytes(
            self.config, content,
            generation=generation, source_name=source_name,
            declared_rows=declared_rows, declared_complete=declared_complete,
        )

        # 重复观察同一文件（同摘要）：直接回放台账，绝不重复发布
        cached = self.store.observation(result.digest)
        if cached is not None:
            return Observation.from_record(cached)

        if result.rejected:
            observation = Observation(
                digest=result.digest, source_name=source_name,
                generation=generation, status='rejected',
                reason=result.reason, errors=result.errors,
                row_count=result.row_count,
                baseline_digest_after=self.store.baseline_digest,
                baseline_generation_after=self.store.baseline_generation,
            )
            self.store.add_observation(observation.to_record())
            return observation

        snapshot = result.snapshot
        self.store.save_snapshot(snapshot)

        baseline_digest = self.store.baseline_digest
        if baseline_digest is None:
            # 第一份确认完整的快照建立基线，本身不产生变更
            self.store.advance_baseline(snapshot)
            observation = Observation(
                digest=result.digest, source_name=source_name,
                generation=generation, status='first_baseline', reason=None,
                errors=(), row_count=result.row_count,
                baseline_digest_after=snapshot.digest,
                baseline_generation_after=snapshot.generation,
            )
            self.store.add_observation(observation.to_record())
            return observation

        baseline_generation = self.store.baseline_generation
        if snapshot.generation < baseline_generation:
            # 较老快照晚到：允许查阅，不推进基线、不产生事件
            observation = Observation(
                digest=result.digest, source_name=source_name,
                generation=generation, status='late', reason='older_than_baseline',
                errors=(), row_count=result.row_count,
                baseline_digest_after=baseline_digest,
                baseline_generation_after=baseline_generation,
            )
            self.store.add_observation(observation.to_record())
            return observation

        baseline = self.store.load_snapshot(baseline_digest)
        diff = diff_snapshots(self.config, baseline, snapshot)
        event_ids, ambiguity_ids = self._persist_diff(diff)
        self.store.advance_baseline(snapshot)
        observation = Observation(
            digest=result.digest, source_name=source_name,
            generation=generation, status='advanced', reason=None,
            errors=(), row_count=result.row_count,
            event_ids=tuple(event_ids), ambiguity_ids=tuple(ambiguity_ids),
            baseline_digest_after=snapshot.digest,
            baseline_generation_after=snapshot.generation,
        )
        self.store.add_observation(observation.to_record())
        return observation

    def observe_file(self, path, **kwargs) -> Observation:
        return self.observe_bytes(Path(path).read_bytes(), **kwargs)

    # ---- 歧义人工确认 --------------------------------------------------
    def confirm_ambiguity(
        self, ambiguity_id: str, *, scheme_index: int, confirmed_by: str,
    ) -> dict:
        record = self.store.get_ambiguity(ambiguity_id)
        if record is None:
            raise KeyError(f'歧义不存在: {ambiguity_id}')
        if not 0 <= scheme_index < len(record['schemes']):
            raise IndexError('方案序号超出范围')
        if record['confirmed']:
            previous = record['confirmation']
            if (previous['scheme_index'] == scheme_index
                    and previous['confirmed_by'] == confirmed_by):
                return {'ambiguity_id': ambiguity_id,
                        'event_ids': list(previous['event_ids']),
                        'idempotent': True}
            raise ValueError('该歧义已被其他确认占用，不能重复解释')

        # 确认绑定输入文件摘要：两份快照必须仍可查阅且与记录一致
        before = self.store.load_snapshot(record['before_snapshot_digest'])
        after = self.store.load_snapshot(record['after_snapshot_digest'])
        if before is None or after is None:
            raise RuntimeError('确认所需的前后快照已不存在，无法绑定文件摘要')
        key = tuple(record['key'])
        scheme = record['schemes'][scheme_index]
        events = self._events_from_scheme(key, scheme, record)
        event_ids = []
        for event in events:
            payload = self.store.add_event(event)
            self._deliver(payload)
            event_ids.append(payload['event_id'])

        confirmation = {
            'scheme_index': scheme_index,
            'confirmed_by': confirmed_by,
            'before_snapshot_digest': record['before_snapshot_digest'],
            'after_snapshot_digest': record['after_snapshot_digest'],
            'event_ids': event_ids,
        }
        self.store.confirm_ambiguity(ambiguity_id, confirmation)
        return {'ambiguity_id': ambiguity_id, 'event_ids': event_ids,
                'idempotent': False}

    # ---- 查询与回收 ----------------------------------------------------
    def get_snapshot(self, digest: str) -> Snapshot | None:
        return self.store.load_snapshot(digest)

    def list_observations(self) -> list[Observation]:
        return [Observation.from_record(r) for r in self.store.list_observations()]

    def list_events(self, status: str | None = None) -> list[dict]:
        return self.store.list_events(status)

    def list_ambiguities(self, include_confirmed: bool = True) -> list[dict]:
        return self.store.list_ambiguities(include_confirmed)

    def ack_event(self, event_id: str) -> bool:
        if self.sink is not None:
            self.sink.ack(event_id)
        return self.store.mark_event_acked(event_id)

    def purge_retained_snapshots(self) -> list[str]:
        return self.store.purge_retained_snapshots()

    # ---- 内部 ----------------------------------------------------------
    def _persist_diff(self, diff: DiffResult):
        event_ids, ambiguity_ids = [], []
        for event in diff.events:
            payload = self.store.add_event(event)
            self._deliver(payload)
            event_ids.append(payload['event_id'])
        for ambiguity in diff.ambiguities:
            payload = self.store.add_ambiguity(ambiguity)
            ambiguity_ids.append(payload['ambiguity_id'])
        return event_ids, ambiguity_ids

    def _deliver(self, event_payload: dict) -> None:
        if self.sink is not None:
            self.sink.deliver(event_payload)

    def _events_from_scheme(self, key: tuple, scheme: dict, record: dict):
        events = []
        for pair in scheme['pairs']:
            changes = pair['changes']
            events.append(ChangeEvent(
                supplier=record['supplier'], kind='changed', key=key,
                changed_columns=tuple(changes.keys()), changes=changes,
                before=Evidence.from_json(pair['before']),
                after=Evidence.from_json(pair['after']),
                before_snapshot_digest=record['before_snapshot_digest'],
                after_snapshot_digest=record['after_snapshot_digest'],
                before_generation=record['before_generation'],
                after_generation=record['after_generation'],
            ))
        for payload in scheme['unmatched_after']:
            events.append(ChangeEvent(
                supplier=record['supplier'], kind='added', key=key,
                changed_columns=(), changes={}, before=None,
                after=Evidence.from_json(payload),
                before_snapshot_digest=record['before_snapshot_digest'],
                after_snapshot_digest=record['after_snapshot_digest'],
                before_generation=record['before_generation'],
                after_generation=record['after_generation'],
            ))
        for payload in scheme['unmatched_before']:
            events.append(ChangeEvent(
                supplier=record['supplier'], kind='removed', key=key,
                changed_columns=(), changes={},
                before=Evidence.from_json(payload), after=None,
                before_snapshot_digest=record['before_snapshot_digest'],
                after_snapshot_digest=record['after_snapshot_digest'],
                before_generation=record['before_generation'],
                after_generation=record['after_generation'],
            ))
        return events
