
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
  auto_increment: bool
  default: typing.Any
  check: str

  UNIQUE_TOKEN = "UNIQUE"
  NOT_NULL_TOKEN = "NOT NULL"
  AUTO_INCREMENT_TOKEN = "AUTOINCREMENT"
  DEFAULT_TOKEN = "DEFAULT"
  CHECK_TOKEN = "CHECK"

  """ Base SQL field implementation """
  def __init__(self,
               name: str,
               data_type_str: str,
               unique: bool = False,
               not_null: bool = False,
               auto_increment: bool = False,
               default: typing.Any=None,
               check: str = None) -> None:
    self.name = name
    self.data_type_str = data_type_str
    self.data_class = SQLField.GetClass(self.data_type_str)
    self.unique = unique
    self.not_null = not_null
    self.auto_increment = auto_increment
    self.default = default
    self.check = check  # Field check constraint

  def __repr__(self) -> str:
    return "<SQLField: '{}' at {:016X}>".format(self.name, id(self))
  
  def GetCreateStr(self):
    if self.auto_increment:
      # Validate type for auto-increment
      normalized_type = self.data_type_str.upper()
      if normalized_type not in ("INT", "INTEGER"):
        raise ValueError(
          f"auto_increment can only be used with INTEGER types, "
          f"got {self.data_type_str} for field {self.name}"
        )
      return f"INTEGER PRIMARY KEY AUTOINCREMENT"
    
    s = self.data_type_str
    subs = []
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
  
  def get_constraint_str(self) -> str:
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
    
  def get_constraint_str(self) -> str:
    """Generate SQL fragment for unique constraint"""
    name_clause = f"CONSTRAINT {self.constraint_name} " if self.constraint_name else ""
    return f"{name_clause}UNIQUE ({', '.join(self.columns)})"

class PrimaryKeyConstraint:
  """Represents table-level primary key constraint"""
  def __init__(self, columns: typing.List[str]):
    self.columns = columns
    
  def get_constraint_str(self) -> str:
    """Generate SQL fragment for primary key constraint"""
    return f"PRIMARY KEY ({', '.join(self.columns)})"

class CheckConstraint:
  """Represents table-level check constraint"""
  def __init__(self, expression: str, constraint_name: str = None):
    self.expression = expression
    self.constraint_name = constraint_name
    
  def get_constraint_str(self) -> str:
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
  
  def get_create_sql(self, table_name: str) -> str:
    """Generate SQL statement to create index"""
    unique_clause = "UNIQUE " if self.unique else ""
    if_not_exists_clause = "IF NOT EXISTS " if self.create_if_not_exists else ""
    columns_str = ", ".join(self.columns)
    
    # Auto-generate index name if not provided
    if not self.name:
      col_names = "_".join([col.replace(" ", "_") for col in self.columns])
      self.name = f"idx_{table_name}_{col_names}"
    
    return f"CREATE {unique_clause}INDEX {if_not_exists_clause}{self.name} ON {table_name}({columns_str});"
  
  def get_drop_sql(self) -> str:
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
  
  def add_field(self, field: SQLField):
    """Add field to table"""
    if field.name in self.field_name_dict:
      raise ValueError(f"Field '{field.name}' already exists in table '{self.name}'")
    
    self.fields.append(field)
    self.field_name_dict[field.name] = field
  
  def set_primary_key(self, columns: typing.Union[str, typing.List[str]]):
    """Set table-level primary key constraint"""
    if isinstance(columns, str):
      columns = [columns]
    
    # Validate all columns exist
    for column in columns:
      if column not in self.field_name_dict:
        raise ValueError(f"Column '{column}' doesn't exist in table")
    
    self.primary_key = PrimaryKeyConstraint(columns)
  
  def add_foreign_key(self, 
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
  
  def add_unique_constraint(self, 
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
  
  def add_check_constraint(self, expression: str, constraint_name: str = None):
    """Add check constraint (table-level)"""
    if not expression.strip():
      raise ValueError("Check constraint expression cannot be empty")
    cc = CheckConstraint(expression, constraint_name)
    self.check_constraints.append(cc)

  def add_index(self, 
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
  
  def get_create_index_sqls(self) -> typing.List[str]:
    """Generate SQL statements to create all indexes"""
    return [index.get_create_sql(self.name) for index in self.indexes]
  
  def get_drop_index_sqls(self) -> typing.List[str]:
    """Generate SQL statements to drop all indexes"""
    return [index.get_drop_sql() for index in self.indexes]
  
  def get_create_table_sql(self) -> str:
    """Generate complete CREATE TABLE statement"""
    field_defs = []
    
    # Field definitions section
    for field in self.fields:
      field_def = f"{field.name} {field.GetCreateStr()}"
      field_defs.append(field_def)
    
    # Table-level constraints section
    constraints = []

    # Check for auto-increment fields (which already include primary key)
    auto_inc_fields = [f for f in self.fields if f.auto_increment]
    
    # Primary key constraint (if no auto-increment fields present)
    if self.primary_key and not auto_inc_fields:
      constraints.append(self.primary_key.get_constraint_str())
    
    # Unique constraints
    for uc in self.unique_constraints:
      constraints.append(uc.get_constraint_str())
    
    # Foreign key constraints
    for fk in self.foreign_keys:
      constraints.append(fk.get_constraint_str())
    
    # Check constraints
    for cc in self.check_constraints:
      constraints.append(cc.get_constraint_str())
    
    # Combine all definitions
    all_defs = field_defs + constraints
    inner = ',\n  '.join(all_defs)
    return f"CREATE TABLE IF NOT EXISTS {self.name} (\n  {inner}\n);"
  
  def get_drop_table_sql(self) -> str:
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
      params = SQLTable._parse_field_string(type_str)
      field = SQLField(
        name=field_name,
        data_type_str=params["data_type"],
        unique=params.get("unique", False),
        not_null=params.get("not_null", False),
        auto_increment=params.get("auto_increment", False),
        default=params.get("default", None),
        check=params.get("check", None)
      )
      table.add_field(field)
    
    # Step 2: Apply table-level constraints
    constr_section = table_def.get("constraints", {})
    
    # Primary key
    pk = constr_section.get("primary_key")
    if pk:
      table.set_primary_key(pk)
    
    # Foreign keys
    fks = constr_section.get("foreign_keys", [])
    for fk_def in fks:
      table.add_foreign_key(
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
      table.add_unique_constraint(columns, name)
    
    # Check constraints
    ccs = constr_section.get("check_constraints", [])
    for cc_def in ccs:
      expression = cc_def["expression"]
      name = cc_def.get("name")
      table.add_check_constraint(expression, name)

    # Add indexes
    indexes = table_def.get("indexes", [])
    for idx_def in indexes:
      columns = idx_def["columns"]
      unique = idx_def.get("unique", False)
      name = idx_def.get("name")
      table.add_index(columns, unique, name)
    
    # Step 3: Validate integrity
    # Check auto-increment fields have proper primary key setup
    auto_inc_fields = [f for f in table.fields if f.auto_increment]
    if auto_inc_fields:
      if len(auto_inc_fields) > 1:
        raise ValueError("Only one auto-increment field is allowed per table")
      
      auto_field = auto_inc_fields[0]
      # Verify table-level primary key includes only this field
      if not table.primary_key:
        table.primary_key = PrimaryKeyConstraint([auto_field.name])
      if table.primary_key.columns != [auto_field.name]:
        raise ValueError(f"Auto-increment field '{auto_field.name}' must be the only primary key")
    
    return table
  
  @staticmethod
  def _parse_field_string(type_str: str) -> dict:
    """
    Parse field definition string to extract parameters
    Example: "INTEGER PRIMARY KEY AUTOINCREMENT" or 
         "TEXT NOT NULL DEFAULT 'abc' CHECK(LENGTH(name) > 5)"
    Returns: {
      "data_type": "TEXT",
      "unique": True,
      "not_null": True,
      "auto_increment": False,
      "default": "'abc'",
      "check": "LENGTH(name) > 5"
    }
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
    
    # Remove processed constraint tokens
    for token in [SQLField.UNIQUE_TOKEN, SQLField.NOT_NULL_TOKEN, 
                  SQLField.AUTO_INCREMENT_TOKEN, "PRIMARY KEY"]:
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
  
  def add_table(self, table: SQLTable):
    """Add table to database structure"""
    if table.name in self.table_name_dict:
      raise ValueError(f"Table '{table.name}' already exists in database")
    
    self.tables.append(table)
    self.table_name_dict[table.name] = table
    
    # Record foreign key dependencies
    for fk in table.foreign_keys:
      self.foreign_key_graph[fk.ref_table].append(table.name)
  
  def get_table(self, name: str) -> typing.Optional[SQLTable]:
    """Get table structure object"""
    return self.table_name_dict.get(name)
  
  def check_foreign_key_cycles(self) -> bool:
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
  
  def generate_sql_script(self) -> str:
    """Generate complete SQL script for database structure"""
    # Check for circular dependencies
    if self.check_foreign_key_cycles():
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
          dep_table = self.get_table(dependent)
          if dep_table:
            queue.append(dep_table)
    
    # Handle remaining tables without foreign key dependencies
    remaining_tables = [table for table in self.tables if table not in sorted_tables]
    sorted_tables.extend(remaining_tables)
    
    # Generate SQL script
    sql_script = []
    
    # Table creation statements
    for table in sorted_tables:
      sql_script.append(table.get_create_table_sql())
    
    # Index creation statements
    for table in sorted_tables:
      for index_sql in table.get_create_index_sqls():
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
      db.add_table(table)
    
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
  
  def get_table_dependencies(self) -> dict:
    """Get dependency relationships between tables"""
    dependencies = {}
    for table in self.tables:
      deps = set()
      for fk in table.foreign_keys:
        deps.add(fk.ref_table)
      dependencies[table.name] = list(deps)
    return dependencies
  
  def get_referencing_tables(self, table_name: str) -> list:
    """Get tables that reference the specified table"""
    return self.foreign_key_graph.get(table_name, [])
  
  def validate_structure(self):
    """Validate integrity of database structure"""
    # Check for foreign key cycles
    if self.check_foreign_key_cycles():
      raise ValueError("Foreign key cycle detected in database structure")
    
    # Verify all foreign key references exist
    for table in self.tables:
      for fk in table.foreign_keys:
        if fk.ref_table not in self.table_name_dict:
          raise ValueError(
            f"Foreign key in table '{table.name}' references "
            f"non-existent table '{fk.ref_table}'"
          )
    
    # Verify auto-increment fields have proper primary key setup
    for table in self.tables:
      auto_inc_fields = [f for f in table.fields if f.auto_increment]
      if auto_inc_fields:
        if len(auto_inc_fields) > 1:
          raise ValueError(
            f"Table '{table.name}' has multiple auto-increment fields"
          )
        
        auto_field = auto_inc_fields[0]
        if not table.primary_key:
          raise ValueError(
            f"Auto-increment field '{auto_field.name}' in table '{table.name}' "
            "must be set as primary key"
          )
        if table.primary_key.columns != [auto_field.name]:
          raise ValueError(
            f"Auto-increment field '{auto_field.name}' in table '{table.name}' "
            "must be the only primary key"
          )

if __name__ == "__main__":
  table_name_initiate_dict = {
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
  test_default_dict = {
    "BasicTable": {
      "field_definition": {
        "id": "INT DEFAULT 2",
        "name": "TEXT"
      }
    }
  }
  test_v2_dict = {
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

  # db = SQLDatabase.CreateFromDict(table_name_initiate_dict)
  # db = SQLDatabase.CreateFromDict(test_default_dict)
  db = SQLDatabase.CreateFromDictV2(test_v2_dict)
  print(db.generate_sql_script())
  pass