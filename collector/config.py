"""供应商比较配置。

每家供应商声明自己的候选键、业务对比列、忽略列与按十进制比较的列；
不同供应商可以复用同一套引擎而互不干扰。
"""

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class SupplierConfig:
    supplier: str
    candidate_key: tuple[str, ...]
    compare_columns: tuple[str, ...]
    ignored_columns: frozenset[str] = field(default_factory=frozenset)
    numeric_columns: frozenset[str] = field(default_factory=frozenset)
    encoding: str = 'utf-8'

    @classmethod
    def from_manifest(cls, manifest: dict) -> 'SupplierConfig':
        return cls(
            supplier=str(manifest['supplier']),
            candidate_key=tuple(manifest['candidate_key']),
            compare_columns=tuple(manifest['compare_columns']),
            ignored_columns=frozenset(manifest.get('ignored_columns', ())),
            numeric_columns=frozenset(manifest.get('numeric_columns', ())),
            encoding=manifest.get('encoding', 'utf-8'),
        )

    def __post_init__(self):
        if not self.supplier:
            raise ValueError('supplier 不能为空')
        if not self.candidate_key:
            raise ValueError('candidate_key 至少包含一列')
        if not self.compare_columns:
            raise ValueError('compare_columns 至少包含一列')
        key_set = set(self.candidate_key)
        if not key_set.issubset(self.compare_columns):
            raise ValueError('候选键必须是业务对比列的子集')
        if self.ignored_columns & set(self.compare_columns):
            raise ValueError('忽略列与对比列不能重叠')
        if not self.numeric_columns.issubset(set(self.compare_columns)):
            raise ValueError('十进制列必须是业务对比列的子集')

    def compare_value(self, column: str, raw: str) -> Decimal | str:
        """数量与金额按十进制比较，其余业务列按字符串比较。"""
        if column in self.numeric_columns:
            if raw is None:
                raise InvalidOperation(f'十进制列 {column} 缺失')
            text = raw.strip()
            if text == '':
                raise InvalidOperation(f'十进制列 {column} 为空')
            return Decimal(text)
        return '' if raw is None else raw
