"""离线端到端演示。

``run()`` 保持原有返回契约（``available_snapshots`` 与
``legacy_delivery_count``），并在其后追加 ``collection`` 采集流程结果。
全部数据来自 fixtures 本地合成文件，状态写入临时目录，重复执行结果完全一致。
"""

import json
import sys
import tempfile
from pathlib import Path

# 兼容 python3 -I：隔离模式不把脚本目录自动加入导入路径
sys.path.insert(0, str(Path(__file__).resolve().parent))

from service import parse_manifest

from collector import CollectionEngine, FileSink, SupplierConfig


def _legacy_delivery_check(root: Path) -> list[dict]:
    manifest = json.loads((root / 'deliveries.json').read_text(encoding='utf-8'))
    parsed = []
    for delivery in manifest['deliveries']:
        digest, rows = parse_manifest(
            (root / 'inbox' / delivery['file']).read_bytes(),
            delivery['encoding'],
        )
        assert digest == delivery['sha256'] and rows
        parsed.append({'file': delivery['file'], 'digest': digest, 'rows': rows})
    assert parsed[1]['digest'] == parsed[2]['digest'] != parsed[3]['digest']
    return parsed


def _available_snapshots(root: Path) -> list[dict]:
    snapshot_manifest = json.loads((root / 'snapshots.json').read_text(encoding='utf-8'))
    snapshots = []
    for entry in snapshot_manifest['files']:
        digest, rows = parse_manifest(
            (root / 'snapshots' / entry['file']).read_bytes()
        )
        snapshots.append({
            'file': entry['file'],
            'digest': digest,
            'rows': rows,
            'declared_rows': entry['declared_rows'],
            'declared_complete': entry['complete'],
        })
    assert len(snapshots[0]['rows']) == 3 and len(snapshots[2]['rows']) == 1
    return snapshots


def _compact(observation) -> dict:
    return {
        'digest': observation.digest,
        'source_name': observation.source_name,
        'generation': observation.generation,
        'status': observation.status,
        'reason': observation.reason,
        'errors': list(observation.errors),
        'row_count': observation.row_count,
        'event_ids': list(observation.event_ids),
        'ambiguity_ids': list(observation.ambiguity_ids),
        'baseline_generation_after': observation.baseline_generation_after,
    }


def _collection_walkthrough(root: Path) -> dict:
    manifest = json.loads((root / 'snapshots.json').read_text(encoding='utf-8'))
    config = SupplierConfig.from_manifest(manifest)
    state_dir = tempfile.mkdtemp(prefix='collector-demo-')
    sink = FileSink(state_dir, config.supplier)
    engine = CollectionEngine(config, state_dir, sink=sink)
    entries = {entry['file']: entry for entry in manifest['files']}

    def observe(file_name: str, *, source_name: str | None = None, **overrides):
        entry = entries[file_name]
        payload = {
            'generation': entry['generation'],
            'source_name': source_name or entry['file'],
            'declared_rows': entry['declared_rows'],
            'declared_complete': entry['complete'],
        }
        payload.update(overrides)
        return engine.observe_bytes(
            (root / 'snapshots' / entry['file']).read_bytes(), **payload
        )

    observations = []

    # 1. 第一份确认完整的快照建立基线
    observations.append(_compact(observe('day1.csv')))

    # 2. 半份文件先以同名 day2.csv 投递：声明不完整，拒绝且不产生删除
    half = observe('day2-partial.csv', source_name='day2.csv')
    observations.append(_compact(half))
    assert half.status == 'rejected' and half.reason == 'source_declared_incomplete'
    assert half.baseline_generation_after == 1

    # 同名完整文件覆盖：行重排 + 数量更正，生成唯一可解释变更
    day2 = observe('day2.csv', source_name='day2.csv')
    observations.append(_compact(day2))
    assert day2.status == 'advanced' and len(day2.event_ids) == 1

    # 3. 歧义快照：同键多个不等价配对方案，挂起待人工确认
    ambiguous = observe('ambiguous.csv')
    observations.append(_compact(ambiguous))
    pending = engine.list_ambiguities(include_confirmed=False)
    assert len(pending) == 1 and len(pending[0]['schemes']) == 2
    ambiguity_id = pending[0]['ambiguity_id']

    # 人工确认：选择方案 0，确认记录绑定前后输入文件摘要
    confirmation = engine.confirm_ambiguity(
        ambiguity_id, scheme_index=0, confirmed_by='ops-reviewer'
    )
    # 同一确认重复提交是幂等的
    again = engine.confirm_ambiguity(
        ambiguity_id, scheme_index=0, confirmed_by='ops-reviewer'
    )
    assert again['idempotent'] and again['event_ids'] == confirmation['event_ids']

    # 4. 重复次数变化：同指纹出现 3 次（多重集合保留次数，而非 set 去重）
    day3 = observe('day3.csv')
    observations.append(_compact(day3))

    # 5. 较老快照晚到（同供应商、不同摘要、generation=1）：可查阅但不回退
    late_bytes = (
        b'sku,qty,price,note\n'
        b'A,9.99,2.50,late-arrival\n'
    )
    late = engine.observe_bytes(
        late_bytes, generation=1, source_name='day1-late-variant.csv',
        declared_rows=1, declared_complete=True,
    )
    observations.append(_compact(late))
    assert late.status == 'late'
    assert engine.store.baseline_generation == 4

    # 6. 重跑：重复观察 day2 同摘要文件，台账回放、不产生新事件/新投递
    published_before = len(engine.list_events())
    rerun = observe('day2.csv', source_name='day2.csv')
    observations.append(_compact(rerun))
    assert rerun.status == 'advanced' and rerun.event_ids == day2.event_ids
    assert len(engine.list_events()) == published_before
    assert len(sink.pending()) == published_before

    # 7. 重启恢复：用同一状态目录重建引擎，事件与投递箱完全一致
    restarted = CollectionEngine(config, state_dir, sink=FileSink(state_dir, config.supplier))
    assert len(restarted.list_events()) == published_before
    assert {e['event_id'] for e in restarted.sink.pending()} == \
           {e['event_id'] for e in sink.pending()}

    # 8. 坏行（数量不是合法十进制）与声明行数不符（截断）都不能成为基线
    legacy_config = SupplierConfig(
        supplier='legacy-vendor',
        candidate_key=('item',),
        compare_columns=('item', 'qty'),
        ignored_columns=frozenset({'at'}),
        numeric_columns=frozenset({'qty'}),
    )
    legacy_engine = CollectionEngine(
        legacy_config, state_dir,
        sink=FileSink(state_dir, legacy_config.supplier),
    )
    bad = legacy_engine.observe_bytes(
        (root / 'inbox' / 'bad-row.csv').read_bytes(),
        generation=1, source_name='bad-row.csv',
        declared_rows=1, declared_complete=True,
    )
    trunc = legacy_engine.observe_bytes(
        (root / 'inbox' / 'part-2.csv').read_bytes(),
        generation=1, source_name='part-2.csv',
        declared_rows=5, declared_complete=True,  # 实际只有 1 行，模拟截断/声明不符
    )
    observations.extend([_compact(bad), _compact(trunc)])
    assert bad.status == 'rejected' and bad.reason == 'bad_row'
    assert trunc.status == 'rejected' and trunc.reason == 'row_count_mismatch'

    # 9. 下游确认前：清理不能删掉待确认事件/歧义引用的基线；
    #    仅无引用的晚到快照可被回收
    required = engine.store.pending_digests()
    purge_before_ack = engine.purge_retained_snapshots()
    assert purge_before_ack == [late.digest]
    assert required.isdisjoint(purge_before_ack)
    for event in engine.list_events(status='pending'):
        assert engine.ack_event(event['event_id'])
    purge_after_ack = engine.purge_retained_snapshots()
    retained = {item['digest'] for item in engine.store.list_snapshots()}
    assert engine.store.baseline_digest in retained
    # 被已 ack 事件引用过的旧基线现在可以回收，但回收集合不得包含当前基线
    assert engine.store.baseline_digest not in purge_after_ack

    return {
        'state_supplier': config.supplier,
        'observations': observations,
        'events': engine.list_events(),
        'ambiguities': engine.list_ambiguities(),
        'ambiguity_confirmation': engine.store.get_ambiguity(ambiguity_id)['confirmation'],
        'outbox_pending': len(sink.pending()),
        'outbox_acked': len(sink.acked()),
        'purge_before_ack': purge_before_ack,
        'purge_after_ack': purge_after_ack,
        'retained_snapshot_digests': sorted(retained),
        'baseline_digest': engine.store.baseline_digest,
    }


def run() -> dict:
    root = Path(__file__).parent / 'fixtures'
    return {
        'available_snapshots': _available_snapshots(root),
        'legacy_delivery_count': len(_legacy_delivery_check(root)),
        'collection': _collection_walkthrough(root),
    }


if __name__ == '__main__':
    print(json.dumps(run(), ensure_ascii=False, indent=2))
