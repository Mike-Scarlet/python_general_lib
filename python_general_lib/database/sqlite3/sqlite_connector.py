
from python_general_lib.database.sqlite3.sqlite_structure import SQLDatabase, SQLTable, SQLField
import sqlite3
import os
import copy
import logging

class SQLite3Connector:
  def __init__(self, path: str, structure: SQLDatabase = None, commit_when_leave: bool = True, 
         verbose_level: int = logging.INFO, foreign_keys: bool = True) -> None:
    """
    SQLite3 database connection manager
    
    Parameters:
      path: Database file path
      structure: Database structure definition (optional)
      commit_when_leave: Whether to automatically commit when object is destroyed
      verbose_level: Logging level
      foreign_keys: Whether to enable foreign key constraints
    """
    self.structure = copy.deepcopy(structure) if structure else SQLDatabase()
    self.path = path
    self.commit_when_leave = commit_when_leave
    self.logger = logging.getLogger("SQLConnector")
    self.logger.setLevel(verbose_level)
    self.conn = None
    self.foreign_keys = foreign_keys
    
  def __getstate__(self):
    return {
      "structure": self.structure,
      "path": self.path,
      "commit_when_leave": self.commit_when_leave
    }

  def __setstate__(self, state):
    self.structure = state["structure"]
    self.path = state["path"]
    self.commit_when_leave = state["commit_when_leave"]
    self.logger = logging.getLogger("SQLConnector")
    self.conn = sqlite3.connect(self.path)
    self._enable_foreign_keys()

  def __enter__(self):
    self.Connect()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type:
      self.logger.error(f"Error occurred: {exc_val}")
      self.rollback()
    elif self.commit_when_leave:
      self.commit()
    self.close()

  def Connect(self, check_same_thread: bool = True, **kwargs) -> None:
    """Connect to database and enable foreign key constraints"""
    if self.conn is None:
      self.conn = sqlite3.connect(
        self.path,
        check_same_thread=check_same_thread,
        detect_types=sqlite3.PARSE_DECLTYPES,
        **kwargs
      )
      self._enable_foreign_keys()
      
      # Initialize structure when creating new database
      if self.path != ":memory:" and not os.path.exists(self.path):
        self.logger.info(f"Creating new database: {self.path}")
        self._initialize_new_database()

  def _enable_foreign_keys(self):
    """Enable foreign key constraint support"""
    if self.foreign_keys and self.conn is not None:
      self.conn.execute("PRAGMA foreign_keys = ON")

  def _initialize_new_database(self):
    """Initialize structure for new database"""
    script = self.structure.generate_sql_script()
    self.conn.executescript(script)
    self.conn.commit()

  def LoadStructureFromDatabase(self) -> None:
    """Load structure definition from existing database"""
    if self.conn is None:
      self.logger.warning("[LoadStructureFromDatabase] Database not connected")
      return
      
    # Get all table names
    cursor = self.conn.cursor()
    cursor.execute("""
      SELECT name FROM sqlite_master 
      WHERE type='table' 
      AND name NOT LIKE 'sqlite_%'
    """)
    table_names = [row[0] for row in cursor.fetchall()]
    
    # Rebuild in-memory database structure
    self.structure = SQLDatabase()
    
    for table_name in table_names:
      table = self._recreate_table_structure(table_name)
      self.structure.add_table(table)
      
    self.logger.info(f"Loaded {len(table_names)} table structures from database")

  def _recreate_table_structure(self, table_name: str) -> SQLTable:
    """Rebuild structure of a single table"""
    table = SQLTable(table_name)
    cursor = self.conn.cursor()
    
    # Get field definitions
    cursor.execute(f"PRAGMA table_info({table_name})")
    for row in cursor.fetchall():
      _, name, data_type, not_null, default_value, pk = row
      
      # Analyze field constraints
      constraints = {
        "unique": False,
        "auto_increment": False,
        "primary_key": False,
        "default": default_value
      }
      
      # Get full creation statement
      cursor.execute(f"SELECT sql FROM sqlite_master WHERE tbl_name = ? AND type = 'table'", (table_name,))
      create_sql = cursor.fetchone()[0].upper()
      
      # Check constraints
      if f"UNIQUE({name})" in create_sql or f"UNIQUE ({name})" in create_sql:
        constraints["unique"] = True
      if "AUTOINCREMENT" in create_sql:
        constraints["auto_increment"] = True
      if pk:
        constraints["primary_key"] = True
      
      # Create field object
      field = SQLField(
        name=name,
        data_type_str=data_type,
        unique=constraints["unique"],
        not_null=bool(not_null),
        auto_increment=constraints["auto_increment"],
        default=default_value
      )
      table.add_field(field)
      
      # Set primary key
      if constraints["primary_key"]:
        table.set_primary_key([name])
    
    # Get foreign key constraints
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    for row in cursor.fetchall():
      _, _, ref_table, from_col, to_col, on_delete, on_update = row
      table.add_foreign_key(
        local_columns=[from_col],
        ref_table=ref_table,
        ref_columns=[to_col],
        on_delete=on_delete,
        on_update=on_update
      )
    
    return table

  def TableValidation(self) -> None:
    """Validate and migrate database structure (auto-migration)"""
    if self.conn is None:
      self.logger.warning("[TableValidation] Database not connected")
      return
      
    # Get current tables in database
    cursor = self.conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing_tables = {row[0] for row in cursor.fetchall()}
    
    # Check table existence
    for table in self.structure.tables:
      if table.name in existing_tables:
        self._validate_table_structure(table)
      else:
        self._create_table(table)
    
    # Handle deprecated tables
    deprecated_tables = existing_tables - {table.name for table in self.structure.tables}
    for table_name in deprecated_tables:
      self.logger.warning(f"Deprecated table detected: {table_name}")

  def _validate_table_structure(self, table: SQLTable):
    """Validate table structure and migrate"""
    existing_fields = self._get_existing_fields(table.name)
    existing_field_names = set(existing_fields.keys())
    
    # Add new fields
    for field in table.fields:
      if field.name not in existing_field_names:
        self.logger.info(f"Adding field {table.name}.{field.name}")
        self._add_field_to_table(table.name, field)
    
    # Field type checking (SQLite doesn't support direct type modification)
    for field in table.fields:
      if field.name in existing_field_names:
        existing_type = existing_fields[field.name]["type"]
        required_type = field.data_type_str.upper()
        
        # Basic type validation
        if existing_type != required_type:
          self.logger.warning(
            f"Field type mismatch: {table.name}.{field.name} "
            f"(Actual: {existing_type}, Required: {required_type}) "
            f"SQLite doesn't support direct field type modification"
          )
    
    # TODO: Handle field deletion and constraint changes (requires complex migration)

  def _get_existing_fields(self, table_name: str) -> dict:
    """Get existing table field information"""
    cursor = self.conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {
      row[1]: {"name": row[1], "type": row[2].upper(), "not_null": row[3]}
      for row in cursor.fetchall()
    }

  def _add_field_to_table(self, table_name: str, field: SQLField):
    """Add new field to table"""
    query = f"ALTER TABLE {table_name} ADD COLUMN {field.name} {field.GetCreateStr()}"
    self.conn.execute(query)
    self.logger.debug(f"Executed: {query}")

  def AddTable(self, table: SQLTable) -> None:
    """Add table to database"""
    if self.conn is None:
      self.logger.warning("[AddTable] Database not connected")
      return
      
    self.structure.add_table(table)
    self._create_table(table)
    self.logger.info(f"Created new table: {table.name}")

  def _create_table(self, table: SQLTable):
    """Create table structure (supports full constraints)"""
    try:
      # Create table structure
      self.conn.execute(table.get_create_table_sql())
      self.logger.debug(f"Executed: {table.get_create_table_sql()}")
      
      # Create indexes
      for index_sql in table.get_create_index_sqls():
        self.conn.execute(index_sql)
        self.logger.debug(f"Executed: {index_sql}")
        
      self.conn.commit()
    except sqlite3.Error as e:
      self.logger.error(f"Failed to create table {table.name}: {str(e)}")
      self.conn.rollback()

  def commit(self):
    """Manually commit transaction"""
    if self.conn:
      self.conn.commit()
      self.logger.debug("Transaction committed")

  def rollback(self):
    """Rollback transaction"""
    if self.conn:
      self.conn.rollback()
      self.logger.debug("Transaction rolled back")

  def close(self):
    """Close database connection"""
    if self.conn:
      if self.commit_when_leave:
        self.commit()
      self.conn.close()
      self.conn = None
      self.logger.info(f"Database connection closed: {self.path}")

  def execute(self, sql: str, params: tuple = None, many: bool = False):
    """Execute SQL query"""
    cursor = self.conn.cursor()
    try:
      if many and isinstance(params, list):
        cursor.executemany(sql, params)
      else:
        cursor.execute(sql, params or ())
      return cursor
    except sqlite3.Error as e:
      self.logger.error(f"SQL execution error: {str(e)}")
      raise

  def __del__(self):
    self.close()

if __name__ == "__main__":
  table_name_initiate_dict = {
    "BasicTable": {
      "field_definition": {
        "id": "INTEGER AUTOINCREMENT",
        "name": "TEXT NOT NULL",
        "time": "REAL"
      },
    },
    "test_table": {
      "field_definition": {
        "id": "INT NOT NULL",
        "id2": "INT",
        "hell": "BLOB"
      },
      "primary_keys": ["id", "hell"]
    }
  }
  db = SQLDatabase.CreateFromDict(table_name_initiate_dict)
  conn = SQLite3Connector("test.db", db)
  conn.Connect()
  # conn.LoadStructureFromDatabase()
  conn.TableValidation()
  conn.commit()
  pass