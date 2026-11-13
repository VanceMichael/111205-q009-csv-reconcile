import json
import unittest

import context  # noqa: F401  python3 -I 下注入仓库根
from collector.diff import diff_snapshots
from collector.models import build_snapshot, normalize_row
from test_collector_models import make_config


def snapshot(config, rows, digest, generation, source_name='f.csv'):
    normalized = [
        normalize_row(config, raw, index)
        for index, raw in enumerate(rows, start=1)
    ]
    return build_snapshot(config, normalized, digest=digest,
                          generation=generation, source_name=source_name)


def row(sku, qty, price, note='n'):
    return {'sku': sku, 'qty': qty, 'price': price, 'note': note}


class DiffTests(unittest.TestCase):
    def setUp(self):
        self.config = make_config()

    def test_reorder_and_ignored_note_produce_no_event(self):
        old = snapshot(self.config, [
            row('A', '1.00', '2.50', 'first'),
            row('A', '1.00', '2.50', 'second'),
            row('B', '0.00', '8.00', 'old'),
        ], 'd1', 1)
        # 完全相同的多重集合，仅物理顺序与备注不同
        new = snapshot(self.config, [
            row('B', '0.00', '8.00', 'new-note'),
            row('A', '1.00', '2.50', 'reordered'),
            row('A', '1.00', '2.50', 'shuffled'),
        ], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(result.events, [])
        self.assertEqual(result.ambiguities, [])

    def test_quantity_correction_is_explainable_change_with_evidence(self):
        old = snapshot(self.config, [
            row('A', '1.00', '2.50'), row('A', '1.00', '2.50'),
            row('B', '0.00', '8.00'),
        ], 'd1', 1)
        new = snapshot(self.config, [
            row('B', '0.00', '8.00'),
            row('A', '1.00', '2.50'), row('A', '2.00', '2.50'),
        ], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(len(result.ambiguities), 0)
        self.assertEqual(len(result.events), 1)
        event = result.events[0]
        self.assertEqual(event.kind, 'changed')
        self.assertEqual(event.key, ('A',))
        self.assertEqual(event.changed_columns, ('qty',))
        self.assertEqual(event.changes['qty'], {'before': '1.00', 'after': '2.00'})
        # 行证据与前后快照摘要随事件携带
        self.assertEqual(event.before.raw['qty'], '1.00')
        self.assertEqual(event.after.raw['qty'], '2.00')
        self.assertEqual(event.before_snapshot_digest, 'd1')
        self.assertEqual(event.after_snapshot_digest, 'd2')

    def test_added_and_removed_rows(self):
        old = snapshot(self.config, [row('A', '1', '2'), row('B', '1', '2')], 'd1', 1)
        new = snapshot(self.config, [row('A', '1', '2'), row('C', '3', '4')], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        kinds = sorted((e.kind, e.key[0]) for e in result.events)
        self.assertEqual(kinds, [('added', 'C'), ('removed', 'B')])

    def test_multiplicity_change_is_counted_not_deduplicated(self):
        old = snapshot(self.config, [
            row('A', '1.50', '2.50'), row('A', '1.75', '2.50'),
        ], 'd1', 1)
        new = snapshot(self.config, [
            row('A', '1.50', '2.50'),
            row('A', '1.50', '2.50'),
            row('A', '1.50', '2.50'),
        ], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(len(result.ambiguities), 0)
        kinds = sorted(e.kind for e in result.events)
        # 唯一配对方案：旧 1.75 与新一个 1.50 配对为变更（旧行被消费），
        # 新侧多出的第二个 1.50 为新增，次数完整保留、不产生悬空移除
        self.assertEqual(kinds, ['added', 'changed'])
        # 多重集合保留次数：新快照里 1.50 的计数是 3，而不是 set 去重后的 1
        only_fp = next(iter(new.multisets[('A',)]))
        self.assertEqual(new.multisets[('A',)][only_fp], 3)

    def test_pure_duplicate_count_increase_is_two_added_events(self):
        # 同指纹从 1 次出现增加到 3 次：只能是两个 added，次数完整保留
        old = snapshot(self.config, [row('A', '1.50', '2.50')], 'd1', 1)
        new = snapshot(self.config, [
            row('A', '1.50', '2.50'),
            row('A', '1.50', '2.50'),
            row('A', '1.50', '2.50'),
        ], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(len(result.ambiguities), 0)
        self.assertEqual(sorted(e.kind for e in result.events),
                         ['added', 'added'])
        self.assertEqual(sum(new.multisets[('A',)].values()), 3)

    def test_multiple_match_schemes_become_ambiguity_not_guesswork(self):
        # 旧 A 两行指纹不同，新 A 两行也不同且交叉：存在两个互异配对方案
        old = snapshot(self.config, [
            row('A', '1.00', '2.50'), row('A', '2.00', '2.50'),
        ], 'd2', 2)
        new = snapshot(self.config, [
            row('A', '3.00', '2.50'), row('A', '4.00', '2.50'),
        ], 'd3', 3)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(result.events, [])
        self.assertEqual(len(result.ambiguities), 1)
        ambiguity = result.ambiguities[0]
        self.assertEqual(ambiguity.reason, 'multiple_match_schemes')
        self.assertEqual(len(ambiguity.schemes), 2)  # 两个互异配对方案
        for scheme in ambiguity.schemes:
            self.assertEqual(len(scheme['pairs']), 2)

    def test_ambiguity_is_independent_of_row_order(self):
        old1 = snapshot(self.config, [
            row('A', '1.00', '2.50'), row('A', '2.00', '2.50'),
        ], 'd1', 1)
        old2 = snapshot(self.config, [
            row('A', '2.00', '2.50'), row('A', '1.00', '2.50'),
        ], 'd1b', 1)
        new = snapshot(self.config, [
            row('A', '3.00', '2.50'), row('A', '4.00', '2.50'),
        ], 'd2', 2)
        r1 = diff_snapshots(self.config, old1, new)
        r2 = diff_snapshots(self.config, old2, new)
        # 物理行序不同，但配对的指纹多重集合（业务语义）完全一致；
        # 证据里的物理行号允许不同，因为行重排本就是预期场景
        def scheme_fingerprints(diff_result):
            return sorted(
                sorted(
                    json.dumps(pair['changes'], sort_keys=True, ensure_ascii=False)
                    for pair in scheme['pairs']
                )
                for scheme in diff_result.ambiguities[0].schemes
            )
        self.assertEqual(scheme_fingerprints(r1), scheme_fingerprints(r2))
        # 两个方案各自都包含两条配对
        self.assertEqual(
            [len(s['pairs']) for s in r1.ambiguities[0].schemes], [2, 2]
        )

    def test_decimal_equivalence_1_50_equals_1_5(self):
        old = snapshot(self.config, [row('A', '1.50', '2.50')], 'd1', 1)
        new = snapshot(self.config, [row('A', '1.5', '2.50')], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(result.events, [])
        self.assertEqual(result.ambiguities, [])

    def test_price_change_is_explained_per_column(self):
        old = snapshot(self.config, [row('A', '1.00', '2.50')], 'd1', 1)
        new = snapshot(self.config, [row('A', '1.00', '3.00')], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(len(result.events), 1)
        self.assertEqual(result.events[0].changed_columns, ('price',))

    def test_materialization_with_duplicate_fingerprints_on_both_sides(self):
        # 旧：X 两次 + Y 一次；新：X 一次 + Z 两次。
        # 一个 X 等价抵消后，旧剩 X、Y；新剩 Z、Z。
        # 最大配对（k=2）是唯一规范方案：X→Z 与 Y→Z 两条变更，无留余。
        old = snapshot(self.config, [
            row('A', '1.00', '2.50'),
            row('A', '1.00', '2.50'),
            row('A', '2.00', '2.50'),
        ], 'd1', 1)
        new = snapshot(self.config, [
            row('A', '1.00', '2.50'),
            row('A', '3.00', '2.50'),
            row('A', '3.00', '2.50'),
        ], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(result.ambiguities, [])
        kinds = sorted(e.kind for e in result.events)
        self.assertEqual(kinds, ['changed', 'changed'])
        changed = result.events
        before_qtys = sorted(e.before.raw['qty'] for e in changed)
        self.assertEqual(before_qtys, ['1.00', '2.00'])
        for event in changed:
            self.assertEqual(event.after.raw['qty'], '3.00')
        # 两条配对各用一个独立的新侧 Z 出现，行证据不重叠
        self.assertEqual(len({e.after.source_row for e in changed}), 2)

    def test_removed_duplicates_keep_each_occurrence(self):
        # 同指纹从 3 次减到 1 次：两个 removed，逐次保留、计数正确，
        # 具体保留哪个物理行号无关紧要（行重排不应影响语义）
        old = snapshot(self.config, [
            row('A', '1.50', '2.50'), row('A', '1.50', '2.50'),
            row('A', '1.50', '2.50'),
        ], 'd1', 1)
        new = snapshot(self.config, [row('A', '1.50', '2.50')], 'd2', 2)
        result = diff_snapshots(self.config, old, new)
        self.assertEqual(result.ambiguities, [])
        removed = result.events
        self.assertEqual(len(removed), 2)
        self.assertTrue(all(e.kind == 'removed' for e in removed))
        self.assertEqual(len({e.before.source_row for e in removed}), 2)
        self.assertEqual(sum(old.multisets[('A',)].values()), 3)
        self.assertEqual(sum(new.multisets[('A',)].values()), 1)


if __name__ == '__main__':
    unittest.main()
