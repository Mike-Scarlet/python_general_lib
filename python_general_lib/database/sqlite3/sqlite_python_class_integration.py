from typing import Any, Dict, List, Optional, Type, Tuple, Union
import datetime
from python_general_lib.database.sqlite3.sqlite_structure import SQLField, SQLTable, SQLDatabase, ForeignKey, UniqueConstraint, Index, PrimaryKeyConstraint
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
               auto_increment: bool = False,
               unique: bool = False, 
               not_null: bool = False, 
               default: Any = None, 
               check: str = None,
               foreign_key: Type = None):
    """
    Initialize SQL field definition
    
    :param primary_key: Whether this is a primary key (field-level primary key definition)
    :param auto_increment: Whether to auto-increment
    :param unique: Whether unique (field-level unique constraint)
    :param not_null: Whether NOT NULL
    :param default: Default value
    :param check: Check constraint expression
    :param foreign_key: Referenced model class
    """
    self.primary_key = primary_key
    self.auto_increment = auto_increment
    self.unique = unique
    self.not_null = not_null
    self.default = default
    self.check = check
    self.foreign_key = foreign_key
  
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
      unique=self.unique,
      not_null=self.not_null or self.primary_key,  # Primary keys are automatically NOT NULL
      auto_increment=self.auto_increment,
      default=processed_default,
      check=self.check
    )

def PySQLModel(cls: Type) -> Type:
  """
  Model decorator
  
  Functions:
  1. Marks a class as an SQL model
  2. Initializes model metadata
  3. Sets default table name
  """

  # Initialize metadata
  if not hasattr(cls, '_sql_meta'):
    cls._sql_meta = {
      'table_name': cls.__name__.lower(),  # Default table name is lowercased class name
      'primary_key': None,  # Table-level primary key constraint
      'unique_constraints': [],  # Table-level unique constraints
      'foreign_keys': [],  # Table-level foreign key constraints
      'indexes': [],  # Index definitions
      'check_constraints': []  # Table-level check constraints
    }
  
  # Check for SQLMeta class definition
  if hasattr(cls, 'SQLMeta'):
    meta = cls.SQLMeta
    
    # Process table name
    if hasattr(meta, 'table_name'):
      cls._sql_meta['table_name'] = meta.table_name
    
    # Process primary key
    if hasattr(meta, 'primary_key'):
      cls._sql_meta['primary_key'] = meta.primary_key
    
    # Process unique constraints
    if hasattr(meta, 'unique_constraints'):
      cls._sql_meta['unique_constraints'] = meta.unique_constraints
    
    # Process foreign key constraints
    if hasattr(meta, 'foreign_keys'):
      cls._sql_meta['foreign_keys'] = meta.foreign_keys
    
    # Process indexes
    if hasattr(meta, 'indexes'):
      cls._sql_meta['indexes'] = meta.indexes
    
    # Process check constraints
    if hasattr(meta, 'check_constraints'):
      cls._sql_meta['check_constraints'] = meta.check_constraints
  
  return cls


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
  foreign_key_refs = {}  # Foreign key reference information
  
  # First pass: Create all table structures
  for model_class in models:
    # Check if decorated
    if not hasattr(model_class, '_sql_meta'):
      raise TypeError(f"Class {model_class.__name__} is not decorated with @PySQLModel")
    
    table_name = model_class._sql_meta['table_name']
    table = _CreateTableFromModel(model_class)
    tables[table_name] = table
    
    # Collect foreign key information
    foreign_key_refs[table_name] = {}
    for field_name, field_def in model_class.__dict__.items():
      if isinstance(field_def, Field) and field_def.foreign_key:
        foreign_key_refs[table_name][field_name] = field_def.foreign_key
  
  # Second pass: Establish foreign key relationships
  for table_name, fk_refs in foreign_key_refs.items():
    table = tables[table_name]
    
    for field_name, ref_model in fk_refs.items():
      # Get referenced table name
      ref_table_name = ref_model._sql_meta['table_name']
      
      # Ensure referenced table exists
      if ref_table_name not in tables:
        raise ValueError(f"Referenced table '{ref_table_name}' not found")
      
      # Add foreign key constraint
      table.AddForeignKey(
        local_columns=field_name,
        ref_table=ref_table_name,
        ref_columns='id'  # Assumes all tables use 'id' as primary key
      )
  
  # Add all tables to database
  for table in tables.values():
    db.AddTable(table)
  
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
  else:
    # Find field-level primary keys
    primary_keys = []
    for field_name, field_def in model_class.__dict__.items():
      if isinstance(field_def, Field) and field_def.primary_key:
        primary_keys.append(field_name)
    
    if primary_keys:
      table.SetPrimaryKey(primary_keys)
  
  # Add unique constraints
  for unique_fields in meta['unique_constraints']:
    table.AddUniqueConstraint(unique_fields)
  
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
  @PySQLModel
  class User:
    """User table model"""
    # Field definitions
    id: int = Field(primary_key=True, auto_increment=True)
    username: str = Field(unique=True, not_null=True)
    email: str = Field(not_null=True)
    created_at: datetime.datetime = Field(default="CURRENT_TIMESTAMP")
    is_active: bool = Field(default=True)
    
    # Table-level definitions
    class SQLMeta:
      table_name = "app_users"  # Custom table name
      indexes = [
        {"columns": ["email"], "unique": True, "name": "idx_user_email"},
        {"columns": ["username"], "name": "idx_user_username"}
      ]
      check_constraints = [
        {"expression": "LENGTH(username) >= 3", "name": "chk_username_length"}
      ]

  @PySQLModel
  class Post:
    """Post table model"""
    # Field definitions
    id: int = Field(primary_key=True, auto_increment=True)
    title: str = Field(not_null=True)
    content: str
    created_at: datetime.datetime = Field(default="CURRENT_TIMESTAMP")
    author_id: int = Field(foreign_key=User)  # Field-level foreign key
    
    # Table-level definitions
    class SQLMeta:
      primary_key = ["id"]  # Explicit table-level primary key
      unique_constraints = [["title", "author_id"]]  # Composite unique constraint
      indexes = [
        {"columns": ["created_at"], "name": "idx_post_created"},
        {"columns": ["title", "created_at"], "name": "idx_post_title_created"}
      ]
      foreign_keys = [  # Table-level foreign keys (supports multi-field)
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

  db = GenerateSQLDatabase(User, Post)

  # Generate SQL script
  sql_script = db.GenerateSQLScript()
  print(sql_script)