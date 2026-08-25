"""PySQLModel 构造 kwargs 校验回归测试。

背景（554baa4 引入的缺陷）：new_init 链式调用时（Child 委托给 Parent 的
new_init），plain-class 分支用"本类自己的注解"校验 kwargs，导致子类自有
字段的 kwarg 被父类拒绝：B(pk=7, extra="x") -> TypeError: A() got
unexpected arguments: kwargs=['extra']。

修复：校验改为按实际实例化类的 MRO 注解全集判断（type(self)），报错信息
也用实例化类名。本文件验证修复及既有拒绝行为不回退。
"""
from python_general_lib.database.sqlite3_wrap import Field, PySQLModel


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


def expect_type_error(fn, needle):
  try:
    fn()
  except TypeError as e:
    assert needle in str(e), "wrong message: {}".format(e)
    return
  raise AssertionError("expected TypeError")


def main():
  # 1. 修复目标：子类自有字段 kwarg + 父类字段 kwarg 混用
  b = Child(pk=7, extra="x")
  assert b.pk == 7 and b.extra == "x" and b.val is None and b.flagged is False
  b2 = Child(extra="y")
  assert b2.extra == "y" and b2.pk is None and b2.flagged is False

  # 2. 未开 inherit_fields 的子类同样不再被父类拒绝（kwargs 沿链生效）
  p = PlainChild(pk=1, other=2)
  assert p.pk == 1 and p.other == 2

  # 3. 拼错/未知字段仍然拒绝，且报错类名是实例化类
  expect_type_error(lambda: Child(pk=1, bogus=2), "Child() got unexpected arguments")
  expect_type_error(lambda: Parent(pk=1, bogus=2), "Parent() got unexpected arguments")

  # 4. 位置参数仍然拒绝（字段不支持按位置传参）
  expect_type_error(lambda: Child(1, 2), "args=(1, 2)")

  print("test_pysqlmodel_init_kwargs: ALL PASSED")


if __name__ == "__main__":
  main()
