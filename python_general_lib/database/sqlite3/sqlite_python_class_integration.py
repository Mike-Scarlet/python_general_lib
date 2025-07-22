
from typing import Any, Dict, List, Optional, Type, Tuple, Union
import datetime
from python_general_lib.database.sqlite3.sqlite_structure import SQLField, SQLTable, SQLDatabase, ForeignKey, UniqueConstraint, Index, PrimaryKeyConstraint
from python_general_lib.interface.json_serializable import IJsonSerializableWithDefault

# 特殊类型映射
TYPE_MAP = {
  int: "INTEGER",
  str: "TEXT",
  float: "REAL",
  bytes: "BLOB",
  bool: "BOOLEAN",
  datetime.date: "DATE",
  datetime.datetime: "DATETIME",
}

class Field:
  """
  SQL字段描述类
  
  功能：封装字段的所有属性和约束
  """
  def __init__(self, 
               primary_key: bool = False, 
               auto_increment: bool = False,
               unique: bool = False, 
               not_null: bool = False, 
               default: Any = None, 
               check: str = None,
               foreign_key: Type = None):
    """
    初始化SQL字段描述
    
    :param primary_key: 是否是主键（字段级主键定义）
    :param auto_increment: 是否自动增长
    :param unique: 是否唯一（字段级唯一约束）
    :param not_null: 是否不允许为null
    :param default: 默认值
    :param check: 检查约束表达式
    :param foreign_key: 引用的模型类
    """
    self.primary_key = primary_key
    self.auto_increment = auto_increment
    self.unique = unique
    self.not_null = not_null
    self.default = default
    self.check = check
    self.foreign_key = foreign_key
  
  def to_sql_field(self, name: str, field_type: Type) -> SQLField:
    """将Python字段转换为SQLField对象"""
    # 获取SQL类型
    sql_type = TYPE_MAP.get(field_type, "TEXT")
    
    # 处理默认值
    processed_default = self.default
    
    # 特殊处理时间类型
    if field_type in (datetime.date, datetime.datetime):
      if self.default == "CURRENT_TIMESTAMP":
        processed_default = "CURRENT_TIMESTAMP"
      elif self.default == "CURRENT_DATE":
        processed_default = "CURRENT_DATE"
      elif self.default == "CURRENT_TIME":
        processed_default = "CURRENT_TIME"
    
    # 处理布尔值默认值
    elif field_type == bool:
      if self.default is True:
        processed_default = 1
      elif self.default is False:
        processed_default = 0
    
    return SQLField(
      name=name,
      data_type_str=sql_type,
      unique=self.unique,
      not_null=self.not_null or self.primary_key,  # 主键自动非空
      auto_increment=self.auto_increment,
      default=processed_default,
      check=self.check
    )


def PySQLModel(cls: Type) -> Type:
  """
  模型装饰器
  
  功能：
  1. 标记类为SQL模型
  2. 初始化模型元数据
  3. 自动设置默认表名
  """
  # inherit from IJsonSerializableWithDefault
  cls.__bases__ = (IJsonSerializableWithDefault,) + cls.__bases__

  # 初始化元数据
  if not hasattr(cls, '_sql_meta'):
    cls._sql_meta = {
      'table_name': cls.__name__.lower(),  # 默认表名为类名小写
      'primary_key': None,  # 表级主键约束
      'unique_constraints': [],  # 表级唯一约束
      'foreign_keys': [],  # 表级外键约束
      'indexes': [],  # 索引定义
      'check_constraints': []  # 表级检查约束
    }
  
  # 检查是否有SQLMeta类定义
  if hasattr(cls, 'SQLMeta'):
    meta = cls.SQLMeta
    
    # 处理表名
    if hasattr(meta, 'table_name'):
      cls._sql_meta['table_name'] = meta.table_name
    
    # 处理主键
    if hasattr(meta, 'primary_key'):
      cls._sql_meta['primary_key'] = meta.primary_key
    
    # 处理唯一约束
    if hasattr(meta, 'unique_constraints'):
      cls._sql_meta['unique_constraints'] = meta.unique_constraints
    
    # 处理外键约束
    if hasattr(meta, 'foreign_keys'):
      cls._sql_meta['foreign_keys'] = meta.foreign_keys
    
    # 处理索引
    if hasattr(meta, 'indexes'):
      cls._sql_meta['indexes'] = meta.indexes
    
    # 处理检查约束
    if hasattr(meta, 'check_constraints'):
      cls._sql_meta['check_constraints'] = meta.check_constraints
  
  return cls


def generate_sql_database(*models: Type) -> SQLDatabase:
  """
  从多个模型类生成SQLDatabase结构
  
  处理流程：
  1. 创建所有表结构（不处理外键）
  2. 建立外键关系
  3. 添加所有表到数据库
  """
  # 创建数据库对象
  db = SQLDatabase()
  tables = {}  # 表名到SQLTable的映射
  foreign_key_refs = {}  # 表的外键引用信息
  
  # 第一遍：创建所有表结构
  for model_class in models:
    # 检查是否应用了装饰器
    if not hasattr(model_class, '_sql_meta'):
      raise TypeError(f"Class {model_class.__name__} is not decorated with @model")
    
    table_name = model_class._sql_meta['table_name']
    table = _create_table_from_model(model_class)
    tables[table_name] = table
    
    # 收集外键信息
    foreign_key_refs[table_name] = {}
    for field_name, field_def in model_class.__dict__.items():
      if isinstance(field_def, Field) and field_def.foreign_key:
        foreign_key_refs[table_name][field_name] = field_def.foreign_key
  
  # 第二遍：建立外键关系
  for table_name, fk_refs in foreign_key_refs.items():
    table = tables[table_name]
    
    for field_name, ref_model in fk_refs.items():
      # 获取引用表的名称
      ref_table_name = ref_model._sql_meta['table_name']
      
      # 确保引用表已创建
      if ref_table_name not in tables:
        raise ValueError(f"Referenced table '{ref_table_name}' not found")
      
      # 添加外键约束
      table.add_foreign_key(
        local_columns=field_name,
        ref_table=ref_table_name,
        ref_columns='id'  # 假设所有表都使用id作为主键
      )
  
  # 添加所有表到数据库
  for table in tables.values():
    db.add_table(table)
  
  return db


def _create_table_from_model(model_class: Type) -> SQLTable:
  """
  从单个模型类创建SQLTable
  
  处理步骤：
  1. 解析字段定义
  2. 设置主键（表级或字段级）
  3. 添加约束和索引
  """
  meta = model_class._sql_meta
  table = SQLTable(meta['table_name'])
  
  # 获取类的所有注解
  annotations = model_class.__annotations__
  
  # 添加字段
  for field_name, field_type in annotations.items():
    # 跳过特殊字段
    if field_name.startswith('__'):
      continue
    
    # 获取字段定义
    field_def = getattr(model_class, field_name, None)
    
    # 如果不是Field实例，创建一个默认的
    if not isinstance(field_def, Field):
      field_def = Field()
    
    # 将Python类型转换为SQL类型
    sql_field = field_def.to_sql_field(field_name, field_type)
    
    # 添加字段到表
    table.add_field(sql_field)
  
  # 设置主键（表级主键优先）
  if meta['primary_key']:
    table.set_primary_key(meta['primary_key'])
  else:
    # 查找字段级主键定义
    primary_keys = []
    for field_name, field_def in model_class.__dict__.items():
      if isinstance(field_def, Field) and field_def.primary_key:
        primary_keys.append(field_name)
    
    if primary_keys:
      table.set_primary_key(primary_keys)
  
  # 添加唯一约束
  for unique_fields in meta['unique_constraints']:
    table.add_unique_constraint(unique_fields)
  
  # 添加外键约束（表级外键）
  for fk_def in meta['foreign_keys']:
    table.add_foreign_key(
      local_columns=fk_def['columns'],
      ref_table=fk_def['ref_table'],
      ref_columns=fk_def['ref_columns'],
      on_delete=fk_def.get('on_delete'),
      on_update=fk_def.get('on_update')
    )
  
  # 添加索引
  for idx_def in meta['indexes']:
    table.add_index(
      columns=idx_def['columns'],
      unique=idx_def.get('unique', False),
      name=idx_def.get('name', None)
    )
  
  # 添加检查约束
  for check_def in meta['check_constraints']:
    table.add_check_constraint(
      expression=check_def['expression'],
      constraint_name=check_def.get('name')
    )
  
  return table

if __name__ == "__main__":
  @PySQLModel
  class User:
    """用户表模型"""
    # 字段定义
    id: int = Field(primary_key=True, auto_increment=True)
    username: str = Field(unique=True, not_null=True)
    email: str = Field(not_null=True)
    created_at: datetime.datetime = Field(default="CURRENT_TIMESTAMP")
    is_active: bool = Field(default=True)
    
    # 表级定义
    class SQLMeta:
      table_name = "app_users"  # 自定义表名
      indexes = [
        {"columns": ["email"], "unique": True, "name": "idx_user_email"},
        {"columns": ["username"], "name": "idx_user_username"}
      ]
      check_constraints = [
        {"expression": "LENGTH(username) >= 3", "name": "chk_username_length"}
      ]

  @PySQLModel
  class Post:
    """帖子表模型"""
    # 字段定义
    id: int = Field(primary_key=True, auto_increment=True)
    title: str = Field(not_null=True)
    content: str
    created_at: datetime.datetime = Field(default="CURRENT_TIMESTAMP")
    author_id: int = Field(foreign_key=User)  # 字段级外键
    
    # 表级定义
    class SQLMeta:
      primary_key = ["id"]  # 显式表级主键定义
      unique_constraints = [["title", "author_id"]]  # 复合唯一约束
      indexes = [
        {"columns": ["created_at"], "name": "idx_post_created"},
        {"columns": ["title", "created_at"], "name": "idx_post_title_created"}
      ]
      foreign_keys = [  # 表级外键定义（支持多字段外键）
        {
          "columns": ["author_id"],
          "ref_table": "app_users",
          "ref_columns": "id",
          "on_delete": "CASCADE"
        }
      ]
      check_constraints = [
        {"expression": "LENGTH(title) > 0", "name": "chk_title_length"}
      ]

  db = generate_sql_database(User, Post)

  # 生成SQL语句
  sql_script = db.generate_sql_script()
  print(sql_script)