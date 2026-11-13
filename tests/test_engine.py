import json
import tempfile
import unittest
from pathlib import Path

import context  # noqa: F401  python3 -I 下注入仓库根
from collector import CollectionEngine, FileSink, SupplierConfig
from test_collector_models import make_config

HEADER = 'sku,qty,price,note\n'


def csv_bytes(lines):
    return (HEADER + ''.join(lines)).encode('utf-8')


class EngineFlowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='collector-test-')
        self.config = make_config()
        self.sink = FileSink(self.tmp, self.config.supplier)
        self.engine = CollectionEngine(self.config, self.tmp, sink=self.sink)

    def observe(self, content, generation, source_name='f.csv',
                declared_rows=None, declared_complete=True):
        if declared_rows is None:
            declared_rows = content.count(b'\n') - 1
        return self.engine.observe_bytes(
            content, generation=generation, source_name=source_name,
            declared_rows=declared_rows, declared_complete=declared_complete,
        )
    def test_half_file_overwrite_then_full_file_advances_baseline(self):
        day1 = csv_bytes(['A,1.00,2.50,x\n', 'A,1.00,2.50,y\n', 'B,0.00,8.00,z\n'])
        half = csv_bytes(['B,0.00,8.00,z\n'])
        full = csv_bytes([
            'B,0.00,8.00,z\n',
            'A,1.00,2.50,r\n',
            'A,2.00,2.50,c\n',
        ])
        self.observe(day1, 1, 'day1.csv', declared_rows=3)
        # 半份以同名文件先投：拒绝，基线不变，无删除事件
        rejected = self.observe(half, 2, 'day2.csv',
                                declared_rows=3, declared_complete=False)
        self.assertEqual(rejected.status, 'rejected')
        self.assertEqual(self.engine.store.baseline_generation, 1)
        self.assertEqual(self.engine.list_events(), [])
        # 同名完整文件覆盖：推进基线，产生唯一变更
        advanced = self.observe(full, 2, 'day2.csv', declared_rows=3)
        self.assertEqual(advanced.status, 'advanced')
        self.assertEqual(self.engine.store.baseline_generation, 2)
        self.assertEqual(len(advanced.event_ids), 1)
        event = self.engine.list_events()[0]
        self.assertEqual(event['kind'], 'changed')
        self.assertEqual(event['changes']['qty'], {'before': '1.00', 'after': '2.00'})
        # 配对证据来自真实行：业务列正确，备注（忽略列）随证据保留
        self.assertEqual(event['before']['raw']['qty'], '1.00')
        self.assertEqual(event['after']['raw']['note'], 'c')
        self.assertEqual(event['before']['source_row'], 1)

    def test_repeated_observation_does_not_republish(self):
        day1 = csv_bytes(['A,1.00,2.50,x\n'])
        day2 = csv_bytes(['A,2.00,2.50,x\n'])
        self.observe(day1, 1)
        first = self.observe(day2, 2)
        events_after_first = len(self.engine.list_events())
        files_after_first = len(list(self.sink.pending_dir.glob('*.json')))
        # 重新观察同一摘要文件
        replay = self.observe(day2, 2)
        self.assertEqual(replay.event_ids, first.event_ids)
        self.assertEqual(len(self.engine.list_events()), events_after_first)
        self.assertEqual(len(list(self.sink.pending_dir.glob('*.json'))),
                         files_after_first)

    def test_restart_recovers_state_and_keeps_single_publication(self):
        day1 = csv_bytes(['A,1.00,2.50,x\n'])
        day2 = csv_bytes(['A,2.00,2.50,x\n', 'C,1.00,1.00,n\n'])
        self.observe(day1, 1)
        self.observe(day2, 2, declared_rows=2)
        events_before = {e['event_id'] for e in self.engine.list_events()}
        restarted_sink = FileSink(self.tmp, self.config.supplier)
        restarted = CollectionEngine(self.config, self.tmp, sink=restarted_sink)
        self.assertEqual(restarted.store.baseline_generation, 2)
        self.assertEqual(
            {e['event_id'] for e in restarted.list_events()}, events_before
        )
        # 重启后再次投递同一文件仍然幂等
        replay = restarted.observe_bytes(
            day2, generation=2, source_name='f.csv', declared_rows=2
        )
        self.assertEqual(replay.status, 'advanced')
        self.assertEqual(len(restarted_sink.pending()), len(events_before))

    def test_ambiguity_confirmation_is_bound_to_file_digests(self):
        day2 = csv_bytes(['A,1.00,2.50,x\n', 'A,2.00,2.50,y\n'])
        day3 = csv_bytes(['A,3.00,2.50,p\n', 'A,4.00,2.50,q\n'])
        self.observe(day2, 2)
        observation = self.observe(day3, 3)
        self.assertEqual(len(observation.ambiguity_ids), 1)
        ambiguity_id = observation.ambiguity_ids[0]
        record = self.engine.store.get_ambiguity(ambiguity_id)
        result = self.engine.confirm_ambiguity(
            ambiguity_id, scheme_index=0, confirmed_by='reviewer-1'
        )
        self.assertEqual(len(result['event_ids']), 2)
        updated = self.engine.store.get_ambiguity(ambiguity_id)
        self.assertTrue(updated['confirmed'])
        # 确认记录绑定了前后两份输入文件的摘要
        self.assertEqual(updated['confirmation']['before_snapshot_digest'],
                         record['before_snapshot_digest'])
        self.assertEqual(updated['confirmation']['after_snapshot_digest'],
                         record['after_snapshot_digest'])
        # 新文件（不同摘要）产生新的歧义 ID，旧确认无法解释新文件
        day3_v2 = csv_bytes(['A,5.00,2.50,p\n', 'A,6.00,2.50,q\n'])
        new_obs = self.observe(day3_v2, 4)
        self.assertEqual(len(new_obs.ambiguity_ids), 1)
        self.assertNotEqual(new_obs.ambiguity_ids[0], ambiguity_id)
        self.assertFalse(
            self.engine.store.get_ambiguity(new_obs.ambiguity_ids[0])['confirmed']
        )

    def test_confirmation_rejects_conflicting_review(self):
        day2 = csv_bytes(['A,1.00,2.50,x\n', 'A,2.00,2.50,y\n'])
        day3 = csv_bytes(['A,3.00,2.50,p\n', 'A,4.00,2.50,q\n'])
        self.observe(day2, 2)
        ambiguity_id = self.observe(day3, 3).ambiguity_ids[0]
        self.engine.confirm_ambiguity(ambiguity_id, scheme_index=0,
                                      confirmed_by='r1')
        with self.assertRaises(ValueError):
            self.engine.confirm_ambiguity(ambiguity_id, scheme_index=1,
                                          confirmed_by='r2')

    def test_late_older_snapshot_is_viewable_but_never_rolls_back(self):
        self.observe(csv_bytes(['A,1.00,2.50,x\n']), 1, 'day1.csv')
        self.observe(csv_bytes(['A,2.00,2.50,x\n']), 2, 'day2.csv')
        late = self.observe(csv_bytes(['A,9.00,2.50,old\n']), 1,
                            'day1-variant.csv')
        self.assertEqual(late.status, 'late')
        self.assertEqual(self.engine.store.baseline_generation, 2)
        # 晚到快照已保存、可查阅
        self.assertIsNotNone(self.engine.get_snapshot(late.digest))
        # 未产生事件、未重复发布
        self.assertEqual(len(self.engine.list_events()), 1)

    def test_baseline_is_retained_until_downstream_acks(self):
        day1_digest = self.observe(csv_bytes(['A,1.00,2.50,x\n']), 1).digest
        self.observe(csv_bytes(['A,2.00,2.50,x\n']), 2)
        # 下游未确认：清理不能移除生成变更所需的旧基线
        self.assertEqual(self.engine.purge_retained_snapshots(), [])
        on_disk = {p.stem for p in self.engine.store.snapshot_dir.glob('*.json')}
        self.assertIn(day1_digest, on_disk)
        for event in self.engine.list_events(status='pending'):
            self.assertTrue(self.engine.ack_event(event['event_id']))
        # 全部 ack 后，旧基线可回收；当前基线始终保留
        removed = self.engine.purge_retained_snapshots()
        self.assertIn(day1_digest, removed)
        remaining = {p.stem for p in self.engine.store.snapshot_dir.glob('*.json')}
        self.assertIn(self.engine.store.baseline_digest, remaining)

    def test_rejected_files_cannot_emit_removal_events(self):
        self.observe(csv_bytes(['A,1.00,2.50,x\n', 'B,1.00,2.50,x\n']), 1,
                     declared_rows=2)
        # 截断：内容与基线不同、声明 5 行实际 2 行
        truncated = self.observe(
            csv_bytes(['A,1.10,2.50,x\n', 'B,1.00,2.50,x\n']), 2,
            declared_rows=5,
        )
        self.assertEqual(truncated.status, 'rejected')
        self.assertEqual(truncated.reason, 'row_count_mismatch')
        self.assertEqual(self.engine.list_events(), [])
        # 坏行：非法十进制
        bad = self.engine.observe_bytes(
            b'sku,qty,price,note\nA,oops,2.50,x\n', generation=2,
            source_name='bad.csv', declared_rows=1,
        )
        self.assertEqual(bad.status, 'rejected')
        self.assertEqual(bad.reason, 'bad_row')
        self.assertEqual(self.engine.list_events(), [])

    def test_event_payload_carries_supplier_snapshots_and_evidence(self):
        self.observe(csv_bytes(['A,1.00,2.50,x\n']), 1, 'old.csv')
        self.observe(csv_bytes(['A,2.00,2.50,y\n']), 2, 'new.csv')
        event = self.engine.list_events()[0]
        self.assertEqual(event['supplier'], self.config.supplier)
        self.assertIn('before_snapshot_digest', event)
        self.assertIn('after_snapshot_digest', event)
        self.assertEqual(event['before']['raw']['sku'], 'A')
        self.assertEqual(event['after']['source_row'], 1)
        # 投递箱内容与状态库一致
        delivered = json.loads(
            (self.sink.pending_dir / f"{event['event_id']}.json").read_text('utf-8')
        )
        self.assertEqual(delivered, event)

    def test_supplier_state_is_isolated(self):
        self.observe(csv_bytes(['A,1.00,2.50,x\n']), 1)
        other_config = make_config(supplier='vendor-y')
        other_sink = FileSink(self.tmp, other_config.supplier)
        other = CollectionEngine(other_config, self.tmp, sink=other_sink)
        # 不同供应商在同一状态根目录下互不可见
        self.assertIsNone(other.store.baseline_digest)
        self.assertEqual(other.list_events(), [])
        other.observe_bytes(
            csv_bytes(['A,9.00,2.50,z\n']), generation=1,
            source_name='f.csv', declared_rows=1,
        )
        self.assertEqual(other.store.baseline_generation, 1)
        # 原供应商状态不受影响
        self.assertEqual(self.engine.store.baseline_generation, 1)
        self.assertEqual(
            self.engine.store.root,
            Path(self.tmp) / self.config.supplier,
        )
        self.assertNotEqual(self.engine.store.root, other.store.root)

    def test_changed_supplier_config_blocks_diff(self):
        from collector.diff import diff_snapshots
        self.observe(csv_bytes(['A,1.00,2.50,x\n']), 1)
        baseline = self.engine.store.load_snapshot(
            self.engine.store.baseline_digest
        )
        reconfigured = make_config(
            numeric_columns=frozenset({'qty'}),  # price 不再按十进制比较
        )
        # 用不同配置构造同内容快照，配置指纹必须不一致并拒绝差分
        from collector.ingestion import ingest_bytes
        result = ingest_bytes(
            reconfigured, csv_bytes(['A,1.00,2.50,x\n']),
            generation=2, source_name='f.csv', declared_rows=1,
        )
        self.assertTrue(result.accepted)
        self.assertNotEqual(
            baseline.config_fingerprint, result.snapshot.config_fingerprint
        )
        with self.assertRaises(ValueError):
            diff_snapshots(reconfigured, baseline, result.snapshot)


if __name__ == '__main__':
    unittest.main()
