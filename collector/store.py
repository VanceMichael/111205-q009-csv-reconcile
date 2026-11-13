"""本地 JSON 持久化。

状态按供应商隔离，全部落盘，进程重启后可完整恢复：

* ``snapshots/<digest>.json``：已接受的完整快照（含多重集合与行证据）；
* ``state.json``：当前基线指针、观察台账、已发布事件与歧义复核记录；
* ``outbox``：下游投递箱，由 :class:`collector.sink.FileSink` 管理。

所有写操作先写临时文件再原子替换，保证重跑与中断恢复不产生半截状态。
"""

import json
import os
from pathlib import Path
from typing import Any

from .diff import Ambiguity, ChangeEvent
from .models import Snapshot


def _canonical(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def content_address(prefix: str, payload: Any) -> str:
    import hashlib
    return f'{prefix}-{hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()[:16]}'


class JsonStore:
    def __init__(self, base_dir: str | os.PathLike, supplier: str):
        self.supplier = supplier
        self.root = Path(base_dir) / supplier
        self.snapshot_dir = self.root / 'snapshots'
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.root / 'state.json'
        self._state = self._load_state()

    # ---- 底层读写 ------------------------------------------------------
    def _load_state(self) -> dict:
        if self.state_path.exists():
            return json.loads(self.state_path.read_text(encoding='utf-8'))
        return {
            'supplier': self.supplier,
            'baseline_digest': None,
            'baseline_generation': None,
            'observations': [],
            'events': [],
            'ambiguities': [],
        }

    def _write_state(self) -> None:
        tmp = self.state_path.with_suffix('.json.tmp')
        tmp.write_text(
            json.dumps(self._state, ensure_ascii=False, indent=2, sort_keys=True),
            encoding='utf-8',
        )
        os.replace(tmp, self.state_path)

    # ---- 观察台账（幂等键 = 文件摘要） --------------------------------
    def observation(self, digest: str) -> dict | None:
        for record in self._state['observations']:
            if record['digest'] == digest:
                return record
        return None

    def add_observation(self, record: dict) -> None:
        self._state['observations'].append(record)
        self._write_state()

    def list_observations(self) -> list[dict]:
        return list(self._state['observations'])

    # ---- 快照 ----------------------------------------------------------
    def save_snapshot(self, snapshot: Snapshot) -> None:
        target = self.snapshot_dir / f'{snapshot.digest}.json'
        if not target.exists():
            tmp = target.with_suffix('.json.tmp')
            tmp.write_text(
                json.dumps(snapshot.to_json(), ensure_ascii=False, indent=2, sort_keys=True),
                encoding='utf-8',
            )
            os.replace(tmp, target)

    def load_snapshot(self, digest: str) -> Snapshot | None:
        target = self.snapshot_dir / f'{digest}.json'
        if not target.exists():
            return None
        return Snapshot.from_json(json.loads(target.read_text(encoding='utf-8')))

    def list_snapshots(self) -> list[dict]:
        return [
            {'digest': p.stem, 'path': str(p)}
            for p in sorted(self.snapshot_dir.glob('*.json'))
        ]

    def delete_snapshot(self, digest: str) -> None:
        target = self.snapshot_dir / f'{digest}.json'
        if target.exists():
            target.unlink()

    # ---- 基线指针（只进不退） ------------------------------------------
    @property
    def baseline_digest(self) -> str | None:
        return self._state['baseline_digest']

    @property
    def baseline_generation(self) -> int | None:
        return self._state['baseline_generation']

    def advance_baseline(self, snapshot: Snapshot) -> None:
        current = self._state['baseline_generation']
        if current is not None and snapshot.generation < current:
            raise ValueError('较老快照不能倒退当前基线')
        self._state['baseline_digest'] = snapshot.digest
        self._state['baseline_generation'] = snapshot.generation
        self._write_state()

    # ---- 事件与歧义 ----------------------------------------------------
    def add_event(self, event: ChangeEvent) -> dict:
        if event.event_id is None:
            event.event_id = content_address('evt', event.body())
        for existing in self._state['events']:
            if existing['event_id'] == event.event_id:
                return existing
        payload = event.to_json()
        payload['status'] = 'pending'
        self._state['events'].append(payload)
        self._write_state()
        return payload

    def mark_event_acked(self, event_id: str) -> bool:
        for event in self._state['events']:
            if event['event_id'] == event_id:
                changed = event['status'] != 'acked'
                event['status'] = 'acked'
                if changed:
                    self._write_state()
                return changed
        return False

    def list_events(self, status: str | None = None) -> list[dict]:
        if status is None:
            return list(self._state['events'])
        return [e for e in self._state['events'] if e['status'] == status]

    def pending_digests(self) -> set[str]:
        refs: set[str] = set()
        for event in self._state['events']:
            if event['status'] != 'acked':
                refs.add(event['before_snapshot_digest'])
                refs.add(event['after_snapshot_digest'])
        for ambiguity in self._state['ambiguities']:
            if not ambiguity['confirmed']:
                refs.add(ambiguity['before_snapshot_digest'])
                refs.add(ambiguity['after_snapshot_digest'])
        return refs

    def add_ambiguity(self, ambiguity: Ambiguity) -> dict:
        if ambiguity.ambiguity_id is None:
            ambiguity.ambiguity_id = content_address('amb', ambiguity.body())
        for existing in self._state['ambiguities']:
            if existing['ambiguity_id'] == ambiguity.ambiguity_id:
                return existing
        payload = ambiguity.to_json()
        self._state['ambiguities'].append(payload)
        self._write_state()
        return payload

    def get_ambiguity(self, ambiguity_id: str) -> dict | None:
        for ambiguity in self._state['ambiguities']:
            if ambiguity['ambiguity_id'] == ambiguity_id:
                return ambiguity
        return None

    def list_ambiguities(self, include_confirmed: bool = True) -> list[dict]:
        if include_confirmed:
            return list(self._state['ambiguities'])
        return [a for a in self._state['ambiguities'] if not a['confirmed']]

    def confirm_ambiguity(self, ambiguity_id: str, confirmation: dict) -> None:
        for ambiguity in self._state['ambiguities']:
            if ambiguity['ambiguity_id'] == ambiguity_id:
                ambiguity['confirmed'] = True
                ambiguity['confirmation'] = confirmation
                self._write_state()
                return
        raise KeyError(ambiguity_id)

    # ---- 保留与清理 ----------------------------------------------------
    def purge_retained_snapshots(self) -> list[str]:
        """删除既不是当前基线、也不被待确认事件/歧义引用的快照。"""
        protected = self.pending_digests()
        if self._state['baseline_digest']:
            protected.add(self._state['baseline_digest'])
        removed = []
        for item in self.list_snapshots():
            digest = item['digest']
            if digest not in protected:
                self.delete_snapshot(digest)
                removed.append(digest)
        return removed
