"""PySQLModel(inherit_fields=True) 注解合并回归测试。

背景：inherit_fields 原先只在 _CreateTableFromModel 建表时走 MRO 合并字段，
运行时读 cls.__annotations__ 的消费者（new_init 字段初始化 / 主键类型查询 /
上层爬虫字段映射）看不到继承字段，导致继承字段（如 OHLC）静默落 NULL。
现在装饰器在 inherit_fields=True 时把 MRO 注解合并进 cls.__annotations__。
"""
from python_general_lib.database.sqlite3_wrap import Field, PySQLModel
from python_general_lib.database.sqlite3_wrap.sqlite_python_class_integration import _CreateTableFromModel


@PySQLModel(initialize_fields=True)
class Parent:
  pk: int = Field(primary_key=True)
  val: float
  flagged: bool = Field(default=False)


@PySQLModel(initialize_fields=True, inherit_fields=True)
class Child(Parent):
  extra: str


@PySQLModel(initialize_fields=True)
class PlainChild(Parent):
  other: int


@PySQLModel(initialize_fields=True)
class Base:
  x: int


@PySQLModel(initialize_fields=True, inherit_fields=True)
class Mid(Base):
  y: int


@PySQLModel(initialize_fields=True, inherit_fields=True)
class Diamond(Mid):
  z: int


def main():
  # 1. inherit_fields=True：注解按 MRO 合并（父类在前，子类覆盖同名）
  assert list(Child.__annotations__) == ["pk", "val", "flagged", "extra"], Child.__annotations__

  # 2. 未开 inherit_fields 的子类不受影响（只有自己的注解）
  assert list(PlainChild.__annotations__) == ["other"], PlainChild.__annotations__

  # 3. 逐级继承合并
  assert list(Diamond.__annotations__) == ["x", "y", "z"], Diamond.__annotations__

  # 4. 实例化：继承字段初始化、Field default 生效
  c = Child(pk=7)
  c.extra = "x"
  assert c.pk == 7 and c.val is None and c.flagged is False and c.extra == "x"

  # 5. 建表：继承字段 + 继承主键不丢
  table = _CreateTableFromModel(Child)
  assert [f.name for f in table.fields] == ["pk", "val", "flagged", "extra"]
  assert [f.name for f in table.fields if f.is_primary] == ["pk"]

  print("test_pysqlmodel_inherit_fields: ALL PASSED")


if __name__ == "__main__":
  main()
