"""下游变更投递箱（本地文件系统实现）。

每个事件按内容寻址 ID 写一个 JSON 文件；重复投递同 ID 直接跳过，
因此重启或重复观察同一文件不会向下游重复发布。
下游确认（ack）后事件移入 ``acked/``，未确认前引擎保留其所需基线。
"""

import json
import os
from pathlib import Path


class FileSink:
    def __init__(self, base_dir: str | os.PathLike, supplier: str):
        self.root = Path(base_dir) / supplier / 'outbox'
        self.pending_dir = self.root / 'pending'
        self.acked_dir = self.root / 'acked'
        self.pending_dir.mkdir(parents=True, exist_ok=True)
        self.acked_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, event_id: str) -> Path:
        return self.pending_dir / f'{event_id}.json'

    def deliver(self, event_payload: dict) -> bool:
        target = self._path(event_payload['event_id'])
        if target.exists() or (self.acked_dir / target.name).exists():
            return False  # 已发布过，幂等跳过
        tmp = target.with_suffix('.json.tmp')
        tmp.write_text(
            json.dumps(event_payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding='utf-8',
        )
        os.replace(tmp, target)
        return True

    def ack(self, event_id: str) -> bool:
        target = self._path(event_id)
        if not target.exists():
            return False
        os.replace(target, self.acked_dir / target.name)
        return True

    def pending(self) -> list[dict]:
        return [
            json.loads(p.read_text(encoding='utf-8'))
            for p in sorted(self.pending_dir.glob('*.json'))
        ]

    def acked(self) -> list[dict]:
        return [
            json.loads(p.read_text(encoding='utf-8'))
            for p in sorted(self.acked_dir.glob('*.json'))
        ]
