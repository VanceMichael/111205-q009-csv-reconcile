"""供应商快照到变更流采集能力。

在 :func:`service.parse_manifest` 提供的 CSV 解码与摘要入口之上，
实现完整快照校验、多重集合差分、歧义复核与幂等发布。
"""

from .config import SupplierConfig
from .diff import Ambiguity, ChangeEvent, diff_snapshots
from .engine import CollectionEngine, Observation
from .ingestion import IngestionResult, ingest_bytes
from .models import NormalizedRow, Snapshot
from .sink import FileSink
from .store import JsonStore

__all__ = [
    'SupplierConfig',
    'NormalizedRow',
    'Snapshot',
    'IngestionResult',
    'ingest_bytes',
    'ChangeEvent',
    'Ambiguity',
    'diff_snapshots',
    'JsonStore',
    'CollectionEngine',
    'Observation',
    'FileSink',
]
