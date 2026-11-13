"""规范化行与快照模型。

快照是按候选键分组的行指纹多重集合：等价行只比较业务列，
忽略列（如备注）不参与指纹；每一行都保留自身出现次数与来源证据。
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

from .config import SupplierConfig


@dataclass(frozen=True)
class Evidence:
    """单行的来源证据：物理行号（从 1 开始的数据行）与原始字段。"""

    source_row: int
    raw: dict

    def to_json(self) -> dict:
        return {'source_row': self.source_row, 'raw': self.raw}

    @classmethod
    def from_json(cls, payload: Mapping) -> 'Evidence':
        return cls(source_row=payload['source_row'], raw=dict(payload['raw']))


@dataclass(frozen=True)
class NormalizedRow:
    key: tuple[str, ...]
    fingerprint: tuple
    evidence: Evidence

    def fingerprint_json(self) -> list:
        return [
            {'type': 'decimal', 'value': str(v)} if isinstance(v, Decimal)
            else {'type': 'text', 'value': v}
            for v in self.fingerprint
        ]

    @classmethod
    def fingerprint_from_json(cls, payload: Sequence) -> tuple:
        return tuple(
            Decimal(item['value']) if item['type'] == 'decimal' else item['value']
            for item in payload
        )


def normalize_row(config: SupplierConfig, raw: Mapping, source_row: int) -> NormalizedRow:
    missing_key = [c for c in config.candidate_key if c not in raw or raw[c] is None]
    if missing_key:
        raise ValueError(f'第 {source_row} 行缺少候选键列: {",".join(missing_key)}')
    key = tuple(str(raw[c]).strip() for c in config.candidate_key)
    if any(part == '' for part in key):
        raise ValueError(f'第 {source_row} 行候选键为空')
    fingerprint = tuple(
        config.compare_value(c, raw.get(c)) for c in config.compare_columns
    )
    return NormalizedRow(
        key=key,
        fingerprint=fingerprint,
        evidence=Evidence(source_row=source_row, raw=dict(raw)),
    )


@dataclass(frozen=True)
class Snapshot:
    digest: str
    generation: int
    source_name: str
    config_fingerprint: tuple
    # key -> {指纹: 出现次数}
    multisets: dict[tuple, dict[tuple, int]]
    # key -> {指纹: [证据...]}，证据按物理行号排列，仅用于人工核对
    evidences: dict[tuple, dict[tuple, list[Evidence]]]
    row_count: int

    def to_json(self) -> dict:
        return {
            'digest': self.digest,
            'generation': self.generation,
            'source_name': self.source_name,
            'config_fingerprint': list(self.config_fingerprint),
            'row_count': self.row_count,
            'multisets': [
                {
                    'key': list(key),
                    'entries': [
                        {'fingerprint': _fp_json(fp), 'count': count}
                        for fp, count in sorted(entries.items(), key=_entry_sort_key)
                    ],
                }
                for key, entries in sorted(self.multisets.items())
            ],
            'evidences': [
                {
                    'key': list(key),
                    'entries': [
                        {'fingerprint': _fp_json(fp),
                         'evidence': [e.to_json() for e in ev_list]}
                        for fp, ev_list in sorted(groups.items(), key=_entry_sort_key)
                    ],
                }
                for key, groups in sorted(self.evidences.items())
            ],
        }

    @classmethod
    def from_json(cls, payload: Mapping) -> 'Snapshot':
        multisets: dict[tuple, dict[tuple, int]] = {}
        evidences: dict[tuple, dict[tuple, list[Evidence]]] = {}
        for bucket in payload['multisets']:
            key = tuple(bucket['key'])
            multisets[key] = {
                NormalizedRow.fingerprint_from_json(entry['fingerprint']): entry['count']
                for entry in bucket['entries']
            }
        for bucket in payload['evidences']:
            key = tuple(bucket['key'])
            evidences[key] = {
                NormalizedRow.fingerprint_from_json(entry['fingerprint']):
                    [Evidence.from_json(e) for e in entry['evidence']]
                for entry in bucket['entries']
            }
        return cls(
            digest=payload['digest'],
            generation=payload['generation'],
            source_name=payload['source_name'],
            config_fingerprint=_normalize_config_fingerprint(payload['config_fingerprint']),
            multisets=multisets,
            evidences=evidences,
            row_count=payload['row_count'],
        )

    def total_count(self, key: tuple) -> int:
        return sum(self.multisets.get(key, {}).values())


def _normalize_config_fingerprint(raw: Sequence) -> tuple:
    """JSON 往返后 frozenset 变成 list，这里恢复成可比较的规范结构。"""
    return tuple(
        tuple(part) if isinstance(part, list) else part
        for part in raw
    )


def _fp_json(fingerprint: tuple) -> list:
    return [
        {'type': 'decimal', 'value': str(v)} if isinstance(v, Decimal)
        else {'type': 'text', 'value': v}
        for v in fingerprint
    ]


def _entry_sort_key(item):
    fp = item[0]
    return tuple(
        (1, str(v)) if isinstance(v, Decimal) else (0, v) for v in fp
    )


def build_snapshot(
    config: SupplierConfig,
    rows: Sequence[NormalizedRow],
    *,
    digest: str,
    generation: int,
    source_name: str,
) -> Snapshot:
    multisets: dict[tuple, dict[tuple, int]] = {}
    evidences: dict[tuple, dict[tuple, list[Evidence]]] = {}
    for row in rows:
        multisets.setdefault(row.key, {}).setdefault(row.fingerprint, 0)
        multisets[row.key][row.fingerprint] += 1
        evidences.setdefault(row.key, {}).setdefault(row.fingerprint, []).append(row.evidence)
    return Snapshot(
        digest=digest,
        generation=generation,
        source_name=source_name,
        config_fingerprint=(
            config.supplier, config.candidate_key, config.compare_columns,
            tuple(sorted(config.ignored_columns)), tuple(sorted(config.numeric_columns)),
        ),
        multisets=multisets,
        evidences=evidences,
        row_count=len(rows),
    )
