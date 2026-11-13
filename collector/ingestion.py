"""快照摄取：在既有 CSV 解析入口上做完整性门禁。

只有同时满足以下条件的文件才能成为对比基线：

1. 编码可解码、CSV 结构完整（无缺列/多列的坏行）；
2. 业务列可按供应商配置规范化（数量金额必须是合法十进制、候选键非空）；
3. 实际数据行数与来源声明行数一致，且来源声明文件完整（未截断）。

被拒绝的文件不会进入快照序列，因此不可能贡献删除事件。
"""

import csv
import io
from dataclasses import dataclass, field

from service import parse_manifest

from .config import SupplierConfig
from .models import NormalizedRow, Snapshot, build_snapshot, normalize_row


@dataclass(frozen=True)
class IngestionResult:
    accepted: bool
    reason: str | None = None
    digest: str | None = None
    row_count: int = 0
    snapshot: Snapshot | None = None
    errors: tuple[str, ...] = field(default_factory=tuple)

    @property
    def rejected(self) -> bool:
        return not self.accepted


def ingest_bytes(
    config: SupplierConfig,
    content: bytes,
    *,
    generation: int,
    source_name: str,
    declared_rows: int | None = None,
    declared_complete: bool = True,
) -> IngestionResult:
    # 复用已有解析入口：摘要与 DictReader 解码行为保持不变
    digest, raw_rows = parse_manifest(content, encoding=config.encoding)
    errors: list[str] = []

    if not declared_complete:
        return IngestionResult(
            accepted=False,
            reason='source_declared_incomplete',
            digest=digest,
            errors=('来源声明该文件不完整（半份投递），拒绝作为基线',),
        )

    text = content.decode(config.encoding)
    reader = csv.reader(io.StringIO(text, newline=''))
    records = list(reader)
    if not records:
        return IngestionResult(
            accepted=False, reason='empty_file', digest=digest,
            errors=('文件为空，缺少表头',),
        )
    header = records[0]
    data_records = records[1:]

    missing_columns = [
        c for c in config.compare_columns if c not in header
    ]
    if missing_columns:
        errors.append(f'表头缺少对比列: {",".join(missing_columns)}')

    normalized: list[NormalizedRow] = []
    seen_bad = False
    for offset, values in enumerate(data_records, start=1):
        if len(values) != len(header):
            errors.append(f'第 {offset} 行字段数 {len(values)} 与表头 {len(header)} 不符（坏行）')
            seen_bad = True
            continue
        raw = dict(zip(header, values))
        try:
            normalized.append(normalize_row(config, raw, offset))
        except Exception as exc:  # 十进制非法、候选键缺失等
            errors.append(f'第 {offset} 行规范化失败: {exc}')
            seen_bad = True

    if declared_rows is not None and len(data_records) != declared_rows:
        errors.append(
            f'实际数据行 {len(data_records)} 与来源声明行数 {declared_rows} 不符（疑似截断或重复投递）'
        )

    if errors:
        return IngestionResult(
            accepted=False,
            reason='bad_row' if seen_bad else 'row_count_mismatch',
            digest=digest,
            row_count=len(data_records),
            errors=tuple(errors),
        )

    snapshot = build_snapshot(
        config, normalized,
        digest=digest, generation=generation, source_name=source_name,
    )
    return IngestionResult(
        accepted=True, digest=digest, row_count=len(normalized), snapshot=snapshot,
    )
