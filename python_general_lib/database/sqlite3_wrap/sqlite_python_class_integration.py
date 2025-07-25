from typing import Any, Dict, List, Optional, Type, Tuple, Union
import datetime
from python_general_lib.database.sqlite3_wrap.sqlite_structure import SQLField, SQLTable, SQLDatabase, ForeignKey, UniqueConstraint, Index, PrimaryKeyConstraint
from python_general_lib.interface.json_serializable import AutoObjectToJsonHandler, AutoObjectFromJsonHander

# SQL type mapping for Python types
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
  SQL field definition class
  
  Encapsulates all attributes and constraints for a database field
  """
  def __init__(self, 
               primary_key: bool = False, 
               unique: bool = False, 
               not_null: bool = False, 
               default: Any = None, 
               check: str = None):
    """
    Initialize SQL field definition
    
    :param primary_key: Whether this is a primary key (field-level primary key definition)
    :param unique: Whether unique (field-level unique constraint)
    :param not_null: Whether NOT NULL
    :param default: Default value
    :param check: Check constraint expression
    """
    self.primary_key = primary_key
    self.unique = unique
    self.not_null = not_null
    self.default = default
    self.check = check
  
  def ToSQLField(self, name: str, field_type: Type) -> SQLField:
    """Convert Python field to SQLField object"""
    # Get SQL type
    sql_type = TYPE_MAP.get(field_type, "TEXT")
    
    # Process default value
    processed_default = self.default
    
    # Handle datetime special cases
    if field_type in (datetime.date, datetime.datetime):
      if self.default == "CURRENT_TIMESTAMP":
        processed_default = "CURRENT_TIMESTAMP"
      elif self.default == "CURRENT_DATE":
        processed_default = "CURRENT_DATE"
      elif self.default == "CURRENT_TIME":
        processed_default = "CURRENT_TIME"
    
    # Handle boolean default values
    elif field_type == bool:
      if self.default is True:
        processed_default = 1
      elif self.default is False:
        processed_default = 0
    
    return SQLField(
      name=name,
      data_type_str=sql_type,
      is_primary=self.primary_key,
      unique=self.unique,
      not_null=self.not_null,
      default=processed_default,
      check=self.check
    )

def PySQLModel(cls: Type = None, *, initialize_fields: bool = False) -> Type:
  """
  Model decorator with field initialization option
  
  Functions:
  1. Marks a class as an SQL model
  2. Initializes model metadata
  3. Sets default table name
  4. Optionally initializes fields on instantiation
  
  Args:
    initialize_fields: If True, initialize all fields with default values (or None) on instantiation
  """
  def decorator(cls: Type) -> Type:
    # Initialize metadata
    if not hasattr(cls, '_sql_meta'):
      cls._sql_meta = {
        'table_name': cls.__name__,  # Default table name is lowercased class name
        'primary_key': None,  # Table-level primary key constraint
        'unique_constraints': [],  # Table-level unique constraints
        'foreign_keys': [],  # Table-level foreign key constraints
        'indexes': [],  # Index definitions
        'check_constraints': []  # Table-level check constraints
      }
    
    # Check for SQLMeta class definition
    if hasattr(cls, 'SQLMeta'):
      meta = cls.SQLMeta
      
      process_keys = [
        "table_name",
        "primary_key",
        "unique_constraints",
        "foreign_keys",
        "indexes",
        "check_constraints",
      ]
      for key in process_keys:
        if hasattr(meta, key):
          cls._sql_meta[key] = getattr(meta, key)

    # Add JSON serialization methods if not defined
    if not hasattr(cls, 'ToJson'):
      def to_json(self) -> Union[dict, list]:
        return AutoObjectToJsonHandler(self)  
      cls.ToJson = to_json

    if not hasattr(cls, 'FromJson'):
      def from_json(self, json_data: Union[dict, list]) -> None:
        return AutoObjectFromJsonHander(self, json_data, allow_not_defined_attr=True)
      cls.FromJson = from_json
    
    # Add field initialization if requested
    if initialize_fields:
      original_init = cls.__init__ if hasattr(cls, '__init__') else None
      
      def new_init(self, *args, **kwargs):
        # Initialize all fields with default values or None
        annotations = cls.__annotations__
        for field_name, field_type in annotations.items():
          if field_name.startswith('__'):
            continue
          
          # Skip if already set by arguments
          if field_name in kwargs:
            continue
          
          # Get field definition if exists
          field_def = getattr(cls, field_name, None)
          default_value = None
          
          if isinstance(field_def, Field):
            # Use field's default value if set
            if field_def.default is not None:
              default_value = field_def.default
          
          setattr(self, field_name, default_value)
        
        # Call original __init__ if exists
        if original_init:
          original_init(self, *args, **kwargs)
      
      cls.__init__ = new_init
    
    return cls
  
  # Handle decorator with or without parentheses
  if cls is None:
    return decorator
  else:
    return decorator(cls)


def GenerateSQLDatabase(*models: Type) -> SQLDatabase:
  """
  Generate SQLDatabase structure from multiple model classes
  
  Process:
  1. Create all table structures (without foreign keys)
  2. Establish foreign key relationships
  3. Add all tables to the database
  """
  # Create database object
  db = SQLDatabase()
  tables = {}  # Table name to SQLTable mapping
  
  # First pass: Create all table structures
  for model_class in models:
    # Check if decorated
    if not hasattr(model_class, '_sql_meta'):
      raise TypeError(f"Class {model_class.__name__} is not decorated with @PySQLModel")
    
    table_name = model_class._sql_meta['table_name']
    table = _CreateTableFromModel(model_class)
    tables[table_name] = table
  
  # Add all tables to database
  for table in tables.values():
    db.AddTable(table)

  for table in db.tables:
    for fk in table.foreign_keys:
      if fk.ref_table not in db.table_name_dict:
        raise ValueError(f"Foreign key references missing table: {fk.ref_table}")
      
  # Check for circular dependencies
  if db.CheckForeignKeyCycles():
    raise RuntimeError("Foreign key cycle detected")
    
  return db


def _CreateTableFromModel(model_class: Type) -> SQLTable:
  """
  Create SQLTable from a single model class
  
  Steps:
  1. Parse field definitions
  2. Set primary key (table-level or field-level)
  3. Add constraints and indexes
  """
  meta = model_class._sql_meta
  table = SQLTable(meta['table_name'])
  
  # Get all class annotations
  annotations = model_class.__annotations__
  
  # Add fields
  for field_name, field_type in annotations.items():
    # Skip special fields
    if field_name.startswith('__'):
      continue
    
    # Get field definition
    field_def = getattr(model_class, field_name, None)
    
    # If not Field instance, create a default one
    if not isinstance(field_def, Field):
      field_def = Field()
    
    # Convert Python type to SQL type
    sql_field = field_def.ToSQLField(field_name, field_type)
    
    # Add field to table
    table.AddField(sql_field)
  
  # Set primary key (table-level primary key has priority)
  if meta['primary_key']:
    table.SetPrimaryKey(meta['primary_key'])
  
  # Add unique constraints
  for unique_fields in meta['unique_constraints']:
    table.AddUniqueConstraint(unique_fields["columns"])
  
  # Add foreign key constraints (table-level)
  for fk_def in meta['foreign_keys']:
    table.AddForeignKey(
      local_columns=fk_def['columns'],
      ref_table=fk_def['ref_table'],
      ref_columns=fk_def['ref_columns'],
      on_delete=fk_def.get('on_delete'),
      on_update=fk_def.get('on_update')
    )
  
  # Add indexes
  for idx_def in meta['indexes']:
    table.AddIndex(
      columns=idx_def['columns'],
      unique=idx_def.get('unique', False),
      name=idx_def.get('name', None)
    )
  
  # Add check constraints
  for check_def in meta['check_constraints']:
    table.AddCheckConstraint(
      expression=check_def['expression'],
      constraint_name=check_def.get('name')
    )
  
  return table

if __name__ == "__main__":
  # 测试用例1: 基本用户模型
  @PySQLModel
  class User:
    id: int
    username: str
    email: str
    created_at: datetime.datetime
    
    class SQLMeta:
      primary_key = "id"
      indexes = [
        {"columns": ["email"], "unique": True},
        {"columns": ["username"], "name": "idx_user_name"}
      ]

  # 测试用例2: 带外键和检查约束的文章模型
  @PySQLModel
  class Post:
    id: int = Field(primary_key=True)
    title: str = Field(not_null=True)
    content: str
    user_id: int
    published: bool = Field(default=False)
    created_at: datetime.datetime = Field(default="CURRENT_TIMESTAMP")
    
    class SQLMeta:
      foreign_keys = [
        {
          "columns": "user_id",
          "ref_table": "User",
          "ref_columns": "id",
          "on_delete": "CASCADE"
        }
      ]
      check_constraints = [
        {"expression": "LENGTH(title) > 5", "name": "chk_title_length"}
      ]

  # 测试用例3: 多列主键和唯一约束的订单模型
  @PySQLModel
  class Order:
    customer_id: int
    product_id: int
    quantity: int = Field(default=1)
    order_date: datetime.date = Field(default="CURRENT_DATE")
    
    class SQLMeta:
      primary_key = ["customer_id", "product_id"]
      unique_constraints = [
        {"columns": ["customer_id", "product_id", "order_date"]}
      ]
      table_name = "orders"  # 自定义表名

  # 测试用例4: 带初始化字段的模型
  @PySQLModel(initialize_fields=True)
  class Config:
    name: str = Field(not_null=True, unique=True)
    value: str
    description: str = Field(default="No description provided")
    active: bool = Field(default=True)

  # 创建数据库结构并生成SQL脚本
  db = GenerateSQLDatabase(User, Post, Order, Config)
  sql_script = db.GenerateSQLScript()
  
  print("=================== GENERATED SQL SCRIPT ===================")
  print(sql_script)
  
  print("\n=================== DATABASE STRUCTURE VALIDATION ===================")
  try:
    db.ValidateStructure()
    print("Database structure is valid")
    
    # 检查外键依赖
    print("\nForeign key relationships:")
    for table in db.tables:
      print(f"Table '{table.name}':")
      if table.foreign_keys:
        for fk in table.foreign_keys:
          print(f"  -> References: {fk.ref_table} ({', '.join(fk.ref_columns)})")
      else:
        print("  No foreign keys")
    
    # 检查外键循环
    if db.CheckForeignKeyCycles():
      print("\nWARNING: Foreign key cycle detected!")
    else:
      print("\nNo foreign key cycles detected")
      
  except ValueError as e:
    print(f"Validation error: {str(e)}")