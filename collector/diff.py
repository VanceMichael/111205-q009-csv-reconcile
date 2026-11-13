"""多重集合快照差分。

规则：

* 行按候选键分组，组内把业务列指纹当作多重集合元素，等价行先按出现次数抵消，
  不使用文件行号，也不使用 set 去重（重复行的次数会被保留）；
* 抵消后一侧为空：另一侧逐次产生新增 / 移除事件；
* 两侧都有剩余：枚举全部**互异的配对方案**（按指纹多重集合去重，
  而不是排序后相邻配对）。方案唯一时产生可解释的数量/金额变更；
  存在多个不等价方案时整组挂起为歧义，交人工确认，引擎不做猜测；
* 忽略列（如备注）不进入指纹，仅备注不同的行视为等价、不产生事件。
"""

from dataclasses import dataclass, field
from decimal import Decimal
from itertools import combinations, permutations
from typing import Mapping

from .config import SupplierConfig
from .models import Evidence, Snapshot

# 单个候选键下参与配对枚举的出现次数上限，超过则直接挂歧义
CANDIDATE_CAP = 12


@dataclass
class ChangeEvent:
    supplier: str
    kind: str  # added | removed | changed
    key: tuple
    changed_columns: tuple[str, ...]
    changes: dict
    before: Evidence | None
    after: Evidence | None
    before_snapshot_digest: str
    after_snapshot_digest: str
    before_generation: int
    after_generation: int
    event_id: str | None = None

    def body(self) -> dict:
        """不含 event_id 的规范载荷，用于生成内容寻址 ID 与序列化。"""
        return {
            'supplier': self.supplier,
            'kind': self.kind,
            'key': list(self.key),
            'changed_columns': list(self.changed_columns),
            'changes': self.changes,
            'before': None if self.before is None else self.before.to_json(),
            'after': None if self.after is None else self.after.to_json(),
            'before_snapshot_digest': self.before_snapshot_digest,
            'after_snapshot_digest': self.after_snapshot_digest,
            'before_generation': self.before_generation,
            'after_generation': self.after_generation,
        }

    def to_json(self) -> dict:
        payload = self.body()
        payload['event_id'] = self.event_id
        return payload


@dataclass
class Ambiguity:
    supplier: str
    key: tuple
    reason: str  # multiple_match_schemes | too_many_candidates
    schemes: list[dict]
    before_snapshot_digest: str
    after_snapshot_digest: str
    before_generation: int
    after_generation: int
    ambiguity_id: str | None = None
    confirmed: bool = False
    confirmation: dict | None = None

    def body(self) -> dict:
        return {
            'supplier': self.supplier,
            'key': list(self.key),
            'reason': self.reason,
            'schemes': self.schemes,
            'before_snapshot_digest': self.before_snapshot_digest,
            'after_snapshot_digest': self.after_snapshot_digest,
            'before_generation': self.before_generation,
            'after_generation': self.after_generation,
        }

    def to_json(self) -> dict:
        payload = self.body()
        payload['ambiguity_id'] = self.ambiguity_id
        payload['confirmed'] = self.confirmed
        payload['confirmation'] = self.confirmation
        return payload


@dataclass
class DiffResult:
    events: list[ChangeEvent] = field(default_factory=list)
    ambiguities: list[Ambiguity] = field(default_factory=list)

    def to_json(self) -> dict:
        return {
            'events': [e.to_json() for e in self.events],
            'ambiguities': [a.to_json() for a in self.ambiguities],
        }


def _scalar(value):
    return str(value) if isinstance(value, Decimal) else value


def _changes(config: SupplierConfig, before_fp: tuple, after_fp: tuple) -> dict:
    out = {}
    for column, old, new in zip(config.compare_columns, before_fp, after_fp):
        if old != new:
            out[column] = {'before': _scalar(old), 'after': _scalar(new)}
    return out


def _occurrences(snapshot: Snapshot, key: tuple) -> tuple[list, list]:
    """展开为逐次出现的 (指纹, 证据) 列表，顺序即物理行号顺序。"""
    fps, evs = [], []
    groups = snapshot.evidences.get(key, {})
    for fp, evidence_list in sorted(groups.items(), key=lambda item: item[1][0].source_row):
        for evidence in evidence_list:
            fps.append(fp)
            evs.append(evidence)
    return fps, evs


def _distinct_schemes(old_fps: list, new_fps: list) -> set:
    """枚举互异的最大配对方案。

    进入本函数的剩余出现已经过等价抵消，新旧两侧不存在共同指纹，
    因此配对只可能是跨指纹的。配对数固定取 ``k = min(n, m)``（最大配对）：
    任何配对更少的方案都会留下本可解释的出现，严格劣于最大配对。

    方案以指纹多重集合规范化为三元组：跨指纹配对多重集合、旧侧未配对
    指纹多重集合、新侧未配对指纹多重集合。同指纹多次出现互换不产生新
    方案，因此结果与物理行顺序无关。
    """
    n, m = len(old_fps), len(new_fps)
    k = min(n, m)
    seen: set = set()
    for old_pick in combinations(range(n), k):
        old_pick_set = set(old_pick)
        old_remaining = tuple(sorted(
            old_fps[i] for i in range(n) if i not in old_pick_set
        ))
        for new_order in permutations(range(m), k):
            cross_pairs = tuple(sorted(
                (old_fps[o], new_fps[p]) for o, p in zip(old_pick, new_order)
            ))
            new_used = set(new_order)
            new_remaining = tuple(sorted(
                new_fps[i] for i in range(m) if i not in new_used
            ))
            seen.add((cross_pairs, old_remaining, new_remaining))
    return seen


def _materialize(config: SupplierConfig, scheme, old_fps, old_evs, new_fps, new_evs) -> dict:
    from collections import Counter

    cross_pairs, old_rest, new_rest = scheme
    old_rest_counter, new_rest_counter = Counter(old_rest), Counter(new_rest)

    def split_occurrences(fps, rest_counter):
        """按物理行号顺序把出现分为「留余」与「可配对」两组，同指纹按序占用。"""
        remaining = Counter(rest_counter)
        pair_slots, leftovers = {}, []
        for index, fp in enumerate(fps):
            if remaining.get(fp, 0) > 0:
                leftovers.append(index)
                remaining[fp] -= 1
            else:
                pair_slots.setdefault(fp, []).append(index)
        return pair_slots, leftovers

    old_pair_slots, old_leftover_idx = split_occurrences(old_fps, old_rest_counter)
    new_pair_slots, new_leftover_idx = split_occurrences(new_fps, new_rest_counter)
    old_cursor, new_cursor = {}, {}

    pair_payload = []
    for old_fp, new_fp in cross_pairs:
        oi = old_cursor.get(old_fp, 0)
        ni = new_cursor.get(new_fp, 0)
        old_index = old_pair_slots[old_fp][oi]
        new_index = new_pair_slots[new_fp][ni]
        old_cursor[old_fp] = oi + 1
        new_cursor[new_fp] = ni + 1
        pair_payload.append({
            'before': old_evs[old_index].to_json(),
            'after': new_evs[new_index].to_json(),
            'changes': _changes(config, old_fp, new_fp),
        })

    return {
        'pairs': pair_payload,
        'unmatched_before': [old_evs[i].to_json() for i in old_leftover_idx],
        'unmatched_after': [new_evs[i].to_json() for i in new_leftover_idx],
    }


def diff_snapshots(
    config: SupplierConfig,
    before: Snapshot,
    after: Snapshot,
) -> DiffResult:
    if before.config_fingerprint != after.config_fingerprint:
        raise ValueError('两份快照的供应商比较配置不一致，无法差分')
    result = DiffResult()
    keys = sorted(set(before.multisets) | set(after.multisets))

    for key in keys:
        old_entries = dict(before.multisets.get(key, {}))
        new_entries = dict(after.multisets.get(key, {}))

        # 第一步：完全等价行按多重集合出现次数抵消
        for fp in set(old_entries) & set(new_entries):
            common = min(old_entries[fp], new_entries[fp])
            old_entries[fp] -= common
            new_entries[fp] -= common
        old_entries = {fp: c for fp, c in old_entries.items() if c}
        new_entries = {fp: c for fp, c in new_entries.items() if c}

        if not old_entries and not new_entries:
            continue

        old_fps, old_evs = _occurrences(before, key)
        new_fps, new_evs = _occurrences(after, key)
        # 只保留抵消后的剩余出现
        old_fps, old_evs = _leftover_occurrences(old_entries, old_fps, old_evs)
        new_fps, new_evs = _leftover_occurrences(new_entries, new_fps, new_evs)

        if not old_fps:
            for fp, ev in zip(new_fps, new_evs):
                result.events.append(ChangeEvent(
                    supplier=config.supplier, kind='added', key=key,
                    changed_columns=(), changes={}, before=None, after=ev,
                    before_snapshot_digest=before.digest,
                    after_snapshot_digest=after.digest,
                    before_generation=before.generation,
                    after_generation=after.generation,
                ))
            continue
        if not new_fps:
            for fp, ev in zip(old_fps, old_evs):
                result.events.append(ChangeEvent(
                    supplier=config.supplier, kind='removed', key=key,
                    changed_columns=(), changes={}, before=ev, after=None,
                    before_snapshot_digest=before.digest,
                    after_snapshot_digest=after.digest,
                    before_generation=before.generation,
                    after_generation=after.generation,
                ))
            continue

        if len(old_fps) + len(new_fps) > CANDIDATE_CAP:
            result.ambiguities.append(Ambiguity(
                supplier=config.supplier, key=key, reason='too_many_candidates',
                schemes=[],
                before_snapshot_digest=before.digest,
                after_snapshot_digest=after.digest,
                before_generation=before.generation,
                after_generation=after.generation,
            ))
            continue

        canonical = _distinct_schemes(old_fps, new_fps)
        if len(canonical) == 1:
            scheme = next(iter(canonical))
            materialized = _materialize(config, scheme, old_fps, old_evs, new_fps, new_evs)
            for pair in materialized['pairs']:
                columns = tuple(pair['changes'].keys())
                result.events.append(ChangeEvent(
                    supplier=config.supplier, kind='changed', key=key,
                    changed_columns=columns, changes=pair['changes'],
                    before=Evidence.from_json(pair['before']),
                    after=Evidence.from_json(pair['after']),
                    before_snapshot_digest=before.digest,
                    after_snapshot_digest=after.digest,
                    before_generation=before.generation,
                    after_generation=after.generation,
                ))
            for payload in materialized['unmatched_after']:
                result.events.append(ChangeEvent(
                    supplier=config.supplier, kind='added', key=key,
                    changed_columns=(), changes={}, before=None,
                    after=Evidence.from_json(payload),
                    before_snapshot_digest=before.digest,
                    after_snapshot_digest=after.digest,
                    before_generation=before.generation,
                    after_generation=after.generation,
                ))
            for payload in materialized['unmatched_before']:
                result.events.append(ChangeEvent(
                    supplier=config.supplier, kind='removed', key=key,
                    changed_columns=(), changes={},
                    before=Evidence.from_json(payload), after=None,
                    before_snapshot_digest=before.digest,
                    after_snapshot_digest=after.digest,
                    before_generation=before.generation,
                    after_generation=after.generation,
                ))
        else:
            schemes = [
                _materialize(config, scheme, old_fps, old_evs, new_fps, new_evs)
                for scheme in sorted(canonical, key=_scheme_sort_key)
            ]
            result.ambiguities.append(Ambiguity(
                supplier=config.supplier, key=key,
                reason='multiple_match_schemes', schemes=schemes,
                before_snapshot_digest=before.digest,
                after_snapshot_digest=after.digest,
                before_generation=before.generation,
                after_generation=after.generation,
            ))

    result.events.sort(key=_event_sort_key)
    result.ambiguities.sort(key=lambda a: a.key)
    return result


def _leftover_occurrences(entries: Mapping, fps: list, evs: list):
    keep_flags = []
    remaining = dict(entries)
    for fp in fps:
        if remaining.get(fp, 0) > 0:
            keep_flags.append(True)
            remaining[fp] -= 1
        else:
            keep_flags.append(False)
    return (
        [fp for fp, keep in zip(fps, keep_flags) if keep],
        [ev for ev, keep in zip(evs, keep_flags) if keep],
    )


def _event_sort_key(event: ChangeEvent):
    before_row = None if event.before is None else event.before.source_row
    after_row = None if event.after is None else event.after.source_row
    return (event.key, event.kind, str(before_row), str(after_row),
            tuple(event.changed_columns))


def _scheme_sort_key(scheme):
    pairs, old_rest, new_rest = scheme
    return (
        tuple((tuple(map(str, o)), tuple(map(str, p))) for o, p in pairs),
        tuple(tuple(map(str, fp)) for fp in old_rest),
        tuple(tuple(map(str, fp)) for fp in new_rest),
    )
