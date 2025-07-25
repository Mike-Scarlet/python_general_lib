
"""
SQLBase

base structure to define sqlite database

abstraction
- SQLite database file
  (has) Table
    (has) Field

the SQL structure should be constructed on program loading,
when it should be read only during whole program process
"""

import re
import typing
import datetime
import collections

"""
#################################
#                               #
#            SQLFIELD           #
#                               #
#################################
"""

class SQLField:
  name: str
  data_type_str: str
  unique: bool
  not_null: bool
  default: typing.Any
  check: str

  PRIMARY_TOKEN = "PRIMARY KEY"
  UNIQUE_TOKEN = "UNIQUE"
  NOT_NULL_TOKEN = "NOT NULL"
  DEFAULT_TOKEN = "DEFAULT"
  CHECK_TOKEN = "CHECK"

  """ Base SQL field implementation """
  def __init__(self,
               name: str,
               data_type_str: str,
               is_primary: bool = False,
               unique: bool = False,
               not_null: bool = False,
               default: typing.Any=None,
               check: str = None) -> None:
    self.name = name
    self.data_type_str = data_type_str
    self.data_class = SQLField.GetClass(self.data_type_str)
    self.is_primary = is_primary
    self.unique = unique
    self.not_null = not_null
    self.default = default
    self.check = check  # Field check constraint

  def __repr__(self) -> str:
    return "<SQLField: '{}' at {:016X}>".format(self.name, id(self))
  
  def GetCreateStr(self):    
    s = self.data_type_str
    subs = []
    if self.is_primary:
      subs.append(SQLField.PRIMARY_TOKEN)
    if self.unique:
      subs.append(SQLField.UNIQUE_TOKEN)
    if self.not_null:
      subs.append(SQLField.NOT_NULL_TOKEN)
    if self.default is not None:
      # Handle special default values
      if self.default == "CURRENT_TIMESTAMP":
        default_val = "CURRENT_TIMESTAMP"
      elif self.default == "CURRENT_DATE":
        default_val = "CURRENT_DATE"
      elif self.default == "CURRENT_TIME":
        default_val = "CURRENT_TIME"
      # Handle boolean values
      elif self.default is True:
        default_val = "1"
      elif self.default is False:
        default_val = "0"
      # Handle numeric values
      elif isinstance(self.default, (int, float)):
        default_val = str(self.default)
      # Handle string values
      elif isinstance(self.default, str):
        # Check if it's a function call or expression
        if self.default.startswith("(") and self.default.endswith(")"):
          default_val = self.default  # Function call, no quotes needed
        else:
          escaped = self.default.replace("'", "''")
          default_val = f"'{escaped}'"
      # Handle other types
      else:
        default_val = str(self.default)
      
      subs.append(f"DEFAULT {default_val}")
    if self.check:
      subs.append(f"CHECK ({self.check})")
    
    if subs:
      s += " " + " ".join(subs)
    return s


  def ParseToSQLData(self, value):
    if value is None:
      return None
    
    # Handle custom types
    if self.data_class == "date":
      return value.isoformat() if value else None
    elif self.data_class == "datetime":
      return value.isoformat() if value else None
    
    # Handle primitive types
    return value
  
  def ParseFromSQLData(self, value):
    try:
      if self.data_class == "date":
        return datetime.date.fromisoformat(value)
      elif self.data_class == "datetime":
        return datetime.datetime.fromisoformat(value)
      
      return value
    except (ValueError, TypeError) as e:
      raise ValueError(f"Error parsing {self.name} value: {value} - {str(e)}")
  
  def ValidateValue(self, value):
    if value is None:
      if self.not_null:
        raise ValueError(f"Field {self.name} cannot be null")
      return
    
    expected_type = {
      int: (int,),
      str: (str,),
      float: (float, int),
      bytes: (bytes,),
      bool: (bool,),
      "date": (datetime.date,),
      "datetime": (datetime.datetime,)
    }.get(self.data_class, (object,))
    
    if not isinstance(value, expected_type):
      raise TypeError(f"Invalid type for field {self.name}. "
                      f"Expected {self.data_class}, got {type(value).__name__}")
    
    # TODO: Implement check constraint validation
    if self.check:
      pass  # Simplified for now

  @staticmethod
  def GetClass(s):
    s_up = s.upper()
    type_map = {
      "INT": int,
      "INTEGER": int,
      "TINYINT": int,
      "SMALLINT": int,
      "MEDIUMINT": int,
      "BIGINT": int,
      "TEXT": str,
      "VARCHAR": str,
      "CHAR": str,
      "CLOB": str,
      "REAL": float,
      "DOUBLE": float,
      "FLOAT": float,
      "NUMERIC": float,
      "DECIMAL": float,
      "BLOB": bytes,
      "BOOLEAN": bool,
      "DATE": "date",
      "DATETIME": "datetime"
    }
    return type_map.get(s_up, None)
    
  @staticmethod
  def GetSQLTypeFromPythonType(python_type):
    type_map = {
      int: "INTEGER",
      str: "TEXT",
      float: "REAL",
      bytes: "BLOB",
      bool: "BOOLEAN",
      "date": "DATE",
      "datetime": "DATETIME"
    }
    return type_map.get(python_type, "TEXT")


"""
#################################
#                               #
#             SQLTABLE          #
#                               #
#################################
"""
class ForeignKey:
  """Represents table-level foreign key constraint"""
  def __init__(self, 
               local_columns: typing.List[str], 
               ref_table: str, 
               ref_columns: typing.List[str],
               on_delete: str = None,
               on_update: str = None):
    self.local_columns = local_columns
    self.ref_table = ref_table
    self.ref_columns = ref_columns
    self.on_delete = on_delete  # Options: 'CASCADE', 'SET NULL', 'NO ACTION', etc.
    self.on_update = on_update
  
  def GetConstraintStr(self) -> str:
    """Generate SQL fragment for foreign key constraint"""
    parts = [
      f"FOREIGN KEY ({', '.join(self.local_columns)})",
      f"REFERENCES {self.ref_table}({', '.join(self.ref_columns)})"
    ]
    
    if self.on_delete:
      parts.append(f"ON DELETE {self.on_delete}")
    if self.on_update:
      parts.append(f"ON UPDATE {self.on_update}")
      
    return " ".join(parts)

class UniqueConstraint:
  """Represents table-level unique constraint"""
  def __init__(self, columns: typing.List[str], constraint_name: str = None):
    self.columns = columns
    self.constraint_name = constraint_name
    
  def GetConstraintStr(self) -> str:
    """Generate SQL fragment for unique constraint"""
    name_clause = f"CONSTRAINT {self.constraint_name} " if self.constraint_name else ""
    return f"{name_clause}UNIQUE ({', '.join(self.columns)})"

class PrimaryKeyConstraint:
  """Represents table-level primary key constraint"""
  def __init__(self, 
               columns: typing.Union[str, typing.List[str]], 
               constraint_type: str = "table"):
    """
    :param columns: Column name(s) included in the primary key
    :param constraint_type: "table" or "column" (indicating origin of the constraint)
    """
    if isinstance(columns, str):
      columns = [columns]
    self.columns = columns
    self.constraint_type = constraint_type
    
  def GetConstraintStr(self) -> str:
    """Generate SQL fragment for primary key constraint"""
    if self.constraint_type == "column":
      # For single-column column-level constraint, just "PRIMARY KEY"
      return ""
    elif self.constraint_type == "table":
      # For table-level constraint or composite keys
      return f"PRIMARY KEY ({', '.join(self.columns)})"
    else:
      raise ValueError(f"Unknown constraint type: {self.constraint_type}")

class CheckConstraint:
  """Represents table-level check constraint"""
  def __init__(self, expression: str, constraint_name: str = None):
    self.expression = expression
    self.constraint_name = constraint_name
    
  def GetConstraintStr(self) -> str:
    """Generate SQL fragment for check constraint"""
    name_clause = f"CONSTRAINT {self.constraint_name} " if self.constraint_name else ""
    return f"{name_clause}CHECK ({self.expression})"

class Index:
  """Represents a database index"""
  def __init__(self, 
               columns: typing.List[str], 
               unique: bool = False, 
               create_if_not_exists: bool = True,
               name: str = None):
    """
    Initialize index
    :param columns: List of column names included in index
    :param unique: Whether index is unique
    :param name: Optional index name
    """
    self.columns = columns
    self.unique = unique
    self.create_if_not_exists = create_if_not_exists
    self.name = name
  
  def GetCreateSQL(self, table_name: str) -> str:
    """Generate SQL statement to create index"""
    unique_clause = "UNIQUE " if self.unique else ""
    if_not_exists_clause = "IF NOT EXISTS " if self.create_if_not_exists else ""
    columns_str = ", ".join(self.columns)
    
    # Auto-generate index name if not provided
    if not self.name:
      col_names = "_".join([col.replace(" ", "_") for col in self.columns])
      self.name = f"idx_{table_name}_{col_names}"
    
    return f"CREATE {unique_clause}INDEX {if_not_exists_clause}{self.name} ON {table_name}({columns_str});"
  
  def GetDropSQL(self) -> str:
    """Generate SQL statement to drop index"""
    return f"DROP INDEX IF EXISTS {self.name};"



class SQLTable:
  fields: list[SQLField]
  field_name_dict: dict[str, SQLField]
  name: str
  primary_key: PrimaryKeyConstraint
  foreign_keys: list[ForeignKey]
  unique_constraints: list[UniqueConstraint]
  check_constraints: list[CheckConstraint]
  indexes: list[Index]
  def __init__(self, name: str) -> None:
    self.fields = []
    self.field_name_dict = {}
    self.name = name
    self.primary_key = None  # Changed to table-level primary key constraint
    self.foreign_keys = []
    self.unique_constraints = []
    self.check_constraints = []
    self.indexes = []
  
  def __repr__(self) -> str:
    return "<SQLTable: '{}' at {:016X}>".format(self.name, id(self))
  
  def AddField(self, field: SQLField):
    """Add field to table"""
    if field.name in self.field_name_dict:
      raise ValueError(f"Field '{field.name}' already exists in table '{self.name}'")
    
    # check column primary key conflict
    if field.is_primary:
      if self.primary_key:
        raise ValueError(f"Cannot add column-level primary key on field '{field.name}' in table '{self.name}': "
                          f"Primary key already exists (type: {self.primary_key.constraint_type})")
      
      # create column primary key
      self.primary_key = PrimaryKeyConstraint(field.name, "column")
    
    self.fields.append(field)
    self.field_name_dict[field.name] = field
  
  def SetPrimaryKey(self, columns: typing.Union[str, typing.List[str]]):
    """Set table-level primary key constraint"""
    if self.primary_key:
      raise ValueError(f"Cannot set table-level primary key in table '{self.name}': "
              f"Primary key already exists (type: {self.primary_key.constraint_type})")
    
    if isinstance(columns, str):
      columns = [columns]
    
    # Validate all columns exist
    for column in columns:
      if column not in self.field_name_dict:
        raise ValueError(f"Column '{column}' doesn't exist in table")
    
    self.primary_key = PrimaryKeyConstraint(columns, "table")
  
  def AddForeignKey(self, 
                      local_columns: typing.Union[str, typing.List[str]], 
                      ref_table: str, 
                      ref_columns: typing.Union[str, typing.List[str]], 
                      on_delete: str = None, 
                      on_update: str = None):
    """Add foreign key constraint"""
    if isinstance(local_columns, str):
      local_columns = [local_columns]
    if isinstance(ref_columns, str):
      ref_columns = [ref_columns]
    
    # Validate column counts match
    if len(local_columns) != len(ref_columns):
      raise ValueError("Local columns and reference columns must have the same count")
    
    # Validate all columns exist
    for col in local_columns:
      if col not in self.field_name_dict:
        raise ValueError(f"Local column '{col}' doesn't exist in table")
    
    fk = ForeignKey(
      local_columns=local_columns,
      ref_table=ref_table,
      ref_columns=ref_columns,
      on_delete=on_delete,
      on_update=on_update
    )
    self.foreign_keys.append(fk)
  
  def AddUniqueConstraint(self, 
                            columns: typing.Union[str, typing.List[str]], 
                            constraint_name: str = None):
    """Add unique constraint (table-level)"""
    if isinstance(columns, str):
      columns = [columns]
    
    for col in columns:
      if col not in self.field_name_dict:
        raise ValueError(f"Column '{col}' doesn't exist in table")
    
    uc = UniqueConstraint(columns, constraint_name)
    self.unique_constraints.append(uc)
  
  def AddCheckConstraint(self, expression: str, constraint_name: str = None):
    """Add check constraint (table-level)"""
    if not expression.strip():
      raise ValueError("Check constraint expression cannot be empty")
    cc = CheckConstraint(expression, constraint_name)
    self.check_constraints.append(cc)

  def AddIndex(self, 
                columns: typing.Union[str, typing.List[str]], 
                unique: bool = False, 
                name: str = None):
    """
    Add index to table
    :param columns: Column name(s) to include in index
    :param unique: Whether index should enforce uniqueness
    :param name: Optional name for the index
    """
    if isinstance(columns, str):
      columns = [columns]
    
    # Validate all columns exist
    for col in columns:
      if col not in self.field_name_dict:
        raise ValueError(f"Column '{col}' doesn't exist in table")
    
    index = Index(columns, unique=unique, name=name)

    if any(idx.name == index.name for idx in self.indexes):
      raise ValueError(f"Index name '{name}' already exists in table")
    self.indexes.append(index)
  
  def GetCreateIndexSQLs(self) -> typing.List[str]:
    """Generate SQL statements to create all indexes"""
    return [index.GetCreateSQL(self.name) for index in self.indexes]
  
  def GetDropIndexSQLs(self) -> typing.List[str]:
    """Generate SQL statements to drop all indexes"""
    return [index.GetDropSQL() for index in self.indexes]
  
  def GetCreateTableSQL(self) -> str:
    """Generate complete CREATE TABLE statement"""
    field_defs = []
    
    # Field definitions section
    for field in self.fields:
      field_def = f"{field.name} {field.GetCreateStr()}"
      field_defs.append(field_def)
    
    # Table-level constraints section
    constraints = []

    # Primary key constraint (if no auto-increment fields present)
    if self.primary_key and self.primary_key.constraint_type == "table":
      constraints.append(self.primary_key.GetConstraintStr())
    
    # Unique constraints
    for uc in self.unique_constraints:
      constraints.append(uc.GetConstraintStr())
    
    # Foreign key constraints
    for fk in self.foreign_keys:
      constraints.append(fk.GetConstraintStr())
    
    # Check constraints
    for cc in self.check_constraints:
      constraints.append(cc.GetConstraintStr())
    
    # Combine all definitions
    all_defs = field_defs + constraints
    inner = ',\n  '.join(all_defs)
    return f"CREATE TABLE IF NOT EXISTS {self.name} (\n  {inner}\n);"
  
  def GetDropTableSQL(self) -> str:
    """Generate DROP TABLE statement"""
    return f"DROP TABLE IF EXISTS {self.name};"
  
  @staticmethod
  def CreateFromDictV2(name: str, table_def: dict) -> "SQLTable":
    """
    Create table from dictionary definition
    Example format:
    {
      "fields": {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "username": "TEXT NOT NULL UNIQUE",
        "email": "TEXT",
        "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
      },
      "constraints": {
        "primary_key": "id",  # or ["col1", "col2"] for composite primary key
        "foreign_keys": [
          {
            "columns": "user_id",
            "ref_table": "users",
            "ref_columns": "id",
            "on_delete": "CASCADE"
          }
        ],
        "unique_constraints": [
          {"columns": ["email", "phone"], "name": "unique_contact"}
        ],
        "check_constraints": [
          {"expression": "age >= 18", "name": "check_adult"}
        ]
      },
      "indexes": [
        {"columns": ["email"], "unique": True},
        {"columns": ["username", "email"], "name": "idx_user_contact"}
      ]
    }
    """
    table = SQLTable(name)
    
    # Step 1: Create fields
    if "fields" not in table_def:
      raise ValueError(f"Table definition must include 'fields' section")
      
    for field_name, type_str in table_def["fields"].items():
      # Parse field definition
      params = SQLTable._ParseFieldString(type_str)
      field = SQLField(
        name=field_name,
        data_type_str=params["data_type"],
        is_primary=params.get("is_primary", False),
        unique=params.get("unique", False),
        not_null=params.get("not_null", False),
        default=params.get("default", None),
        check=params.get("check", None)
      )
      table.AddField(field)
    
    # Step 2: Apply table-level constraints
    constr_section = table_def.get("constraints", {})
    
    # Primary key
    pk = constr_section.get("primary_key")
    if pk:
      table.SetPrimaryKey(pk)
    
    # Foreign keys
    fks = constr_section.get("foreign_keys", [])
    for fk_def in fks:
      table.AddForeignKey(
        local_columns=fk_def["columns"],
        ref_table=fk_def["ref_table"],
        ref_columns=fk_def["ref_columns"],
        on_delete=fk_def.get("on_delete"),
        on_update=fk_def.get("on_update")
      )
    
    # Unique constraints
    ucs = constr_section.get("unique_constraints", [])
    for uc_def in ucs:
      columns = uc_def["columns"]
      name = uc_def.get("name")
      table.AddUniqueConstraint(columns, name)
    
    # Check constraints
    ccs = constr_section.get("check_constraints", [])
    for cc_def in ccs:
      expression = cc_def["expression"]
      name = cc_def.get("name")
      table.AddCheckConstraint(expression, name)

    # Add indexes
    indexes = table_def.get("indexes", [])
    for idx_def in indexes:
      columns = idx_def["columns"]
      unique = idx_def.get("unique", False)
      name = idx_def.get("name")
      table.AddIndex(columns, unique, name)
    
    return table
  
  @staticmethod
  def _ParseFieldString(type_str: str) -> dict:
    """
    Parse field definition string to extract parameters
    """
    # Preprocess: uppercase for matching but keep original values
    normalized = type_str.upper()
    params = {
      "data_type": type_str.strip().split(" ", 1)[0],
      "unique": "UNIQUE" in normalized,
      "not_null": "NOT NULL" in normalized,
      "auto_increment": "AUTOINCREMENT" in normalized,
      "is_primary": "PRIMARY KEY" in normalized
    }

    if params["auto_increment"]:
      raise ValueError("AUTOINCREMENT is not allowed in this framework")
    
    # Remove processed constraint tokens
    for token in [SQLField.UNIQUE_TOKEN, SQLField.NOT_NULL_TOKEN, 
                  "AUTOINCREMENT", "PRIMARY KEY"]:
      normalized = normalized.replace(token, "")
    
    # Parse DEFAULT clause
    default_match = re.search(r"DEFAULT\s+([^,\)]+)", normalized, re.IGNORECASE)
    if default_match:
      params["default"] = default_match.group(1).strip()
      # Remove from normalized string
      normalized = normalized.replace(default_match.group(0), "")
    
    # Parse CHECK constraint
    check_match = re.search(r"CHECK\s*\((.+?)\)", normalized, re.IGNORECASE)
    if check_match:
      # Get expression from original string
      params["check"] = type_str[check_match.start(1):check_match.end(1)]
      normalized = normalized.replace(check_match.group(0), "")
    
    return params


"""
#################################
#                               #
#          SQLDatabase          #
#                               #
#################################
"""

class SQLDatabase:
  tables: list[SQLTable]
  table_name_dict: dict[str, SQLTable]

  def __init__(self):
    """
    Pure container class representing database structure
    Does not contain actual database connection or operations
    """
    self.tables = []
    self.table_name_dict = {}
    self.foreign_key_graph = collections.defaultdict(list)  # For storing foreign key dependencies
  
  def __repr__(self) -> str:
    return f"<SQLDatabase with {len(self.tables)} tables at {id(self):016X}>"
  
  def AddTable(self, table: SQLTable):
    """Add table to database structure"""
    if table.name in self.table_name_dict:
      raise ValueError(f"Table '{table.name}' already exists in database")
    
    self.tables.append(table)
    self.table_name_dict[table.name] = table
    
    # Record foreign key dependencies
    for fk in table.foreign_keys:
      self.foreign_key_graph[fk.ref_table].append(table.name)
  
  def GetTable(self, name: str) -> typing.Optional[SQLTable]:
    """Get table structure object"""
    return self.table_name_dict.get(name)
  
  def CheckForeignKeyCycles(self) -> bool:
    """Check for circular foreign key dependencies (structural check only)"""
    visited = set()
    rec_stack = set()
    
    def dfs(table_name):
      visited.add(table_name)
      rec_stack.add(table_name)
      
      for neighbor in self.foreign_key_graph.get(table_name, []):
        if neighbor not in visited:
          if dfs(neighbor):
            return True
        elif neighbor in rec_stack:
          return True
      
      rec_stack.remove(table_name)
      return False
    
    for table in self.tables:
      if table.name not in visited:
        if dfs(table.name):
          return True
    return False
  
  def GenerateSQLScript(self) -> str:
    """Generate complete SQL script for database structure"""
    # Check for circular dependencies
    if self.CheckForeignKeyCycles():
      raise RuntimeError("Foreign key cycle detected")
    
    # Build dependency graph (TableA depends on TableB = TableA references TableB)
    dependency_graph = collections.defaultdict(list)
    in_degree = {table.name: 0 for table in self.tables}
    
    # Calculate in-degree for each table (number of dependencies)
    for table in self.tables:
      for fk in table.foreign_keys:
        # TableA (current table) depends on TableB (referenced table)
        dependency_graph[fk.ref_table].append(table.name)
        in_degree[table.name] += 1
    
    # Topological sort
    sorted_tables = []
    queue = [table for table in self.tables if in_degree[table.name] == 0]
    
    while queue:
      table = queue.pop(0)
      sorted_tables.append(table)
      
      # Reduce in-degree for dependent tables
      for dependent in dependency_graph.get(table.name, []):
        in_degree[dependent] -= 1
        if in_degree[dependent] == 0:
          dep_table = self.GetTable(dependent)
          if dep_table:
            queue.append(dep_table)
    
    # Handle remaining tables without foreign key dependencies
    remaining_tables = [table for table in self.tables if table not in sorted_tables]
    sorted_tables.extend(remaining_tables)
    
    # Generate SQL script
    sql_script = []
    
    # Table creation statements
    for table in sorted_tables:
      sql_script.append(table.GetCreateTableSQL())
    
    # Index creation statements
    for table in sorted_tables:
      for index_sql in table.GetCreateIndexSQLs():
        sql_script.append(index_sql)
    
    return "\n\n".join(sql_script)
  
  @staticmethod
  def CreateFromDictV2(db_def: dict) -> "SQLDatabase":
    """
    Create database structure from dictionary definition
    Example format:
    {
      "users": {
        "fields": {
          "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
          "username": "TEXT NOT NULL UNIQUE",
          "email": "TEXT",
          "group_id": "INTEGER",
          "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
        },
        "constraints": {
          "primary_key": "id",
          "foreign_keys": [
            {
            "columns": "group_id",
            "ref_table": "user_groups",
            "ref_columns": "id",
            "on_delete": "SET NULL"
            }
          ]
        },
        "indexes": [
          {"columns": ["email"], "unique": True}
        ]
      },
      "user_groups": {
        "fields": {
          "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
          "name": "TEXT NOT NULL"
        }
      }
    }
    """
    db = SQLDatabase()
    
    # Create all table objects first
    for table_name, table_def in db_def.items():
      table = SQLTable.CreateFromDictV2(table_name, table_def)
      db.AddTable(table)
    
    # Verify referenced tables exist for foreign keys
    for table in db.tables:
      for fk in table.foreign_keys:
        if fk.ref_table not in db.table_name_dict:
          raise ValueError(
            f"Foreign key in table '{table.name}' references "
            f"non-existent table '{fk.ref_table}'"
          )
    
    return db
  
  @staticmethod
  def CreateFromDict(old_db_def: dict) -> "SQLDatabase":
    """
    Create database structure from legacy dictionary format (backward compatible)
    Legacy format example:
    {
      "BasicTable": {
        "field_definition": {
          "id": "INTEGER NOT NULL AUTOINCREMENT",
          "name": "TEXT",
          "time": "REAL"
        },
        "primary_keys": "id"
      },
      "test_table": {
        "field_definition": {
          "id": "INT NOT NULL",
          "hell": "BLOB"
        }
      }
    }
    """
    # Convert legacy format to new format
    new_db_def = {}
    
    for table_name, table_def in old_db_def.items():
      # Convert field definitions
      fields = table_def["field_definition"]
      
      # Convert constraints
      constraints = {}
      if "primary_keys" in table_def:
        constraints["primary_key"] = table_def["primary_keys"]
      
      # Build new format table definition
      new_table_def = {
        "fields": fields,
        "constraints": constraints
      }
      
      new_db_def[table_name] = new_table_def
    
    # Use new format creation method
    return SQLDatabase.CreateFromDictV2(new_db_def)
  
  def GetTableDependencies(self) -> dict:
    """Get dependency relationships between tables"""
    dependencies = {}
    for table in self.tables:
      deps = set()
      for fk in table.foreign_keys:
        deps.add(fk.ref_table)
      dependencies[table.name] = list(deps)
    return dependencies
  
  def GetReferencingTables(self, table_name: str) -> list:
    """Get tables that reference the specified table"""
    return self.foreign_key_graph.get(table_name, [])
  
  def ValidateStructure(self):
    """Validate integrity of database structure"""
    # Check for foreign key cycles
    if self.CheckForeignKeyCycles():
      raise ValueError("Foreign key cycle detected in database structure")
    
    # Verify all foreign key references exist
    for table in self.tables:
      for fk in table.foreign_keys:
        if fk.ref_table not in self.table_name_dict:
          raise ValueError(
            f"Foreign key in table '{table.name}' references "
            f"non-existent table '{fk.ref_table}'"
          )

if __name__ == "__main__":
  # 测试用例1: 基本表结构 (旧格式)
  test_case1 = {
    "users": {
      "field_definition": {
        "id": "INTEGER NOT NULL",
        "username": "TEXT NOT NULL",
        "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
      },
      "primary_keys": "id"
    },
    "posts": {
      "field_definition": {
        "id": "INTEGER NOT NULL",
        "title": "TEXT NOT NULL",
        "content": "TEXT",
        "user_id": "INTEGER",
      }
    }
  }
  
  # 测试用例2: 新格式带表级约束
  test_case2 = {
    "users": {
      "fields": {
        "id": "INTEGER",
        "username": "TEXT NOT NULL UNIQUE",
        "email": "TEXT",
        "created_at": "DATETIME DEFAULT CURRENT_TIMESTAMP",
      },
      "constraints": {
        "primary_key": "id",
        "check_constraints": [
          {"expression": "LENGTH(username) > 3", "name": "chk_username_length"}
        ]
      },
      "indexes": [
        {"columns": ["email"], "unique": True}
      ]
    },
    "posts": {
      "fields": {
        "id": "INTEGER PRIMARY KEY",
        "title": "TEXT NOT NULL",
        "content": "TEXT",
        "user_id": "INTEGER",
      },
      "constraints": {
        "foreign_keys": [
          {
            "columns": "user_id",
            "ref_table": "users",
            "ref_columns": "id",
            "on_delete": "CASCADE"
          }
        ]
      }
    }
  }
  
  # 测试用例3: 高级场景带多列约束
  test_case3 = {
    "departments": {
      "fields": {
        "dept_id": "INTEGER PRIMARY KEY",
        "dept_name": "TEXT NOT NULL",
      }
    },
    "employees": {
      "fields": {
        "emp_id": "INTEGER PRIMARY KEY",
        "first_name": "TEXT NOT NULL",
        "last_name": "TEXT NOT NULL",
        "salary": "REAL",
        "dept_id": "INTEGER",
        "start_date": "DATE DEFAULT CURRENT_DATE",
      },
      "constraints": {
        "unique_constraints": [
          {"columns": ["first_name", "last_name"], "name": "uq_full_name"}
        ],
        "foreign_keys": [
          {
            "columns": "dept_id",
            "ref_table": "departments",
            "ref_columns": "dept_id",
            "on_update": "CASCADE"
          }
        ],
        "check_constraints": [
          {"expression": "salary > 0", "name": "chk_salary_positive"}
        ]
      },
      "indexes": [
        {"columns": ["last_name"], "name": "idx_employees_lastname"},
        {"columns": ["dept_id", "salary"], "name": "idx_dept_salary"}
      ]
    }
  }
  
  # 测试用例4: 默认值特殊处理
  test_case4 = {
    "settings": {
      "fields": {
        "id": "INTEGER PRIMARY KEY",
        "is_active": "BOOLEAN DEFAULT 1",
        "config": "TEXT DEFAULT '{}'",
        "created": "DATETIME DEFAULT CURRENT_TIMESTAMP",
        "modified": "DATETIME"
      }
    }
  }

  print("==================== 测试用例1: 旧格式基本表 ====================")
  db1 = SQLDatabase.CreateFromDict(test_case1)
  print(db1.GenerateSQLScript())
  
  print("\n==================== 测试用例2: 新格式带外键约束 ====================")
  db2 = SQLDatabase.CreateFromDictV2(test_case2)
  print(db2.GenerateSQLScript())
  
  print("\n==================== 测试用例3: 复杂表级约束 ====================")
  db3 = SQLDatabase.CreateFromDictV2(test_case3)
  print(db3.GenerateSQLScript())
  
  print("\n==================== 测试用例4: 特殊默认值处理 ====================")
  db4 = SQLDatabase.CreateFromDictV2(test_case4)
  print(db4.GenerateSQLScript())
  
  # 验证外键循环检测
  print("\n==================== 测试外键循环检测 ====================")
  cyclic_db_def = {
    "tableA": {
      "fields": {
        "id": "INTEGER PRIMARY KEY",
        "b_id": "INTEGER",
      },
      "constraints": {
        "foreign_keys": [
          {"columns": "b_id", "ref_table": "tableB", "ref_columns": "id"}
        ]
      }
    },
    "tableB": {
      "fields": {
        "id": "INTEGER PRIMARY KEY",
        "a_id": "INTEGER",
      },
      "constraints": {
        "foreign_keys": [
          {"columns": "a_id", "ref_table": "tableA", "ref_columns": "id"}
        ]
      }
    }
  }
  
  try:
    cyclic_db = SQLDatabase.CreateFromDictV2(cyclic_db_def)
    print("意外成功: 外键循环未被检测到")
  except RuntimeError as e:
    print(f"成功捕获外键循环错误: {str(e)}")
  pass