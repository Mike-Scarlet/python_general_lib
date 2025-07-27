
from python_general_lib.database.sqlite3_wrap.sqlite_structure import SQLDatabase, SQLTable, SQLField
import sqlite3
import os, re
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
    self._EnableForeignKeys()

  def __enter__(self):
    self.Connect()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if exc_type:
      self.logger.error(f"Error occurred: {exc_val}")
      self.Rollback()
    elif self.commit_when_leave:
      self.Commit()
    self.Close()

  def Connect(self, check_same_thread: bool = True, **kwargs) -> None:
    """Connect to database and enable foreign key constraints"""
    if self.conn is None:
      self.conn = sqlite3.connect(
        self.path,
        check_same_thread=check_same_thread,
        detect_types=sqlite3.PARSE_DECLTYPES,
        **kwargs
      )
      self._EnableForeignKeys()
      
      # Initialize structure when creating new database
      if self.path != ":memory:" and not os.path.exists(self.path):
        self.logger.info(f"Creating new database: {self.path}")
        self._InitializeNewDatabase()

  def _EnableForeignKeys(self):
    """Enable foreign key constraint support"""
    if self.foreign_keys and self.conn is not None:
      self.conn.execute("PRAGMA foreign_keys = ON")

  def _InitializeNewDatabase(self):
    """Initialize structure for new database"""
    script = self.structure.GenerateSQLScript()
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
      table = self._RecreateTableStructure(table_name)
      self.structure.AddTable(table)
      
    self.logger.info(f"Loaded {len(table_names)} table structures from database")

  def _RecreateTableStructure(self, table_name: str) -> SQLTable:
    table = SQLTable(table_name)
    cursor = self.conn.cursor()
    
    # 获取字段定义
    cursor.execute(f"PRAGMA table_info({table_name})")
    for row in cursor.fetchall():
      _, name, data_type, not_null, default_value, pk = row
      field = SQLField(
        name=name,
        data_type_str=data_type,
        not_null=bool(not_null),
        default=default_value,
        is_primary=False  # 稍后统一处理主键
      )
      table.AddField(field)
    
    # 获取主键约束 - 使用大小写不敏感匹配
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE tbl_name=? AND type='table'", (table_name,))
    row = cursor.fetchone()
    if row is None:
      create_sql = ""
    else:
      create_sql = row[0]
    
    # 使用大小写不敏感的正则表达式
    pk_match = re.search(r"PRIMARY KEY\s*\(([^)]+)\)", create_sql, re.IGNORECASE)
    if pk_match:
      # 保留原始大小写
      pk_columns = [col.strip().replace('"', '') for col in pk_match.group(1).split(",")]
      table.SetPrimaryKey(pk_columns)
    
    # 获取外键约束
    cursor.execute(f"PRAGMA foreign_key_list({table_name})")
    fk_groups = {}
    for row in cursor.fetchall():
      group_id, _, ref_table, from_col, to_col, on_update, on_delete = row[:7]
      if group_id not in fk_groups:
        fk_groups[group_id] = {'local': [], 'ref': [], 'ref_table': ref_table}
      fk_groups[group_id]['local'].append(from_col)
      fk_groups[group_id]['ref'].append(to_col)
      fk_groups[group_id]['on_update'] = on_update
      fk_groups[group_id]['on_delete'] = on_delete
    
    for fk in fk_groups.values():
      table.AddForeignKey(
        fk['local'],
        fk['ref_table'],
        fk['ref'],
        fk.get('on_delete'),
        fk.get('on_update')
      )
    
    # 获取唯一约束 - 使用大小写不敏感匹配
    unique_matches = re.findall(r"CONSTRAINT\s+(\w+)\s+UNIQUE\s*\(([^)]+)\)", create_sql, re.IGNORECASE)
    for name, cols in unique_matches:
      # 保留原始大小写
      table.AddUniqueConstraint([c.strip() for c in cols.split(",")], name)
    
    # 获取检查约束 - 使用大小写不敏感匹配
    check_matches = re.findall(r"CONSTRAINT\s+(\w+)\s+CHECK\s*\(([^)]+)\)", create_sql, re.IGNORECASE)
    for name, expr in check_matches:
      # 保留原始大小写
      table.AddCheckConstraint(expr, name)
    
    # 获取索引 - 修复后的实现
    cursor.execute(f"PRAGMA index_list({table_name})")
    index_list = cursor.fetchall()
    
    for index_row in index_list:
      # index_row 结构: (seq, name, unique, origin, partial)
      index_name = index_row[1]
      unique = bool(index_row[2])
      origin = index_row[3]
      
      # 跳过主键索引（因为主键约束已经处理过）
      if origin == 'pk':
        continue
      
      # 获取索引包含的列
      cursor.execute(f"PRAGMA index_info({index_name})")
      index_cols = []
      for col_row in cursor.fetchall():
        # col_row 结构: (seqno, cid, name)
        seqno = col_row[0]
        col_name = col_row[2]
        index_cols.append((seqno, col_name))
      
      # 按seqno排序并提取列名
      index_cols.sort(key=lambda x: x[0])
      columns = [col[1] for col in index_cols]
      
      # 添加索引到表结构
      table.AddIndex(columns, unique=unique, name=index_name)
    
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
        self._ValidateTableStructure(table)
        # 添加索引验证
        self._ValidateTableIndexes(table)
      else:
        self._CreateTable(table)
    
    # Handle deprecated tables
    deprecated_tables = existing_tables - {table.name for table in self.structure.tables}
    for table_name in deprecated_tables:
      if table_name == "sqlite_sequence":
        continue  # default for AUTOINCREMENT
      self.logger.warning(f"Deprecated table detected: {table_name}")

  def _ValidateTableStructure(self, table: SQLTable):
    """Validate table structure and migrate"""
    existing_fields = self._GetExistingFields(table.name)
    existing_field_names = set(existing_fields.keys())
    
    # Add new fields
    for field in table.fields:
      if field.name not in existing_field_names:
        self.logger.info(f"Adding field {table.name}.{field.name}")
        self._AddFieldToTable(table.name, field)
    
    # Field type checking (SQLite doesn't support direct type modification)
    for field in table.fields:
      if field.name in existing_field_names:
        existing_type = existing_fields[field.name]["type"]
        required_type = field.data_type_str
        
        # Basic type validation
        if existing_type.upper() != required_type.upper():
          self.logger.warning(
            f"Field type mismatch: {table.name}.{field.name} "
            f"(Actual: {existing_type}, Required: {required_type}) "
            f"SQLite doesn't support direct field type modification"
          )
    
    # TODO: Handle field deletion and constraint changes (requires complex migration)
    
  def _ValidateTableIndexes(self, table: SQLTable):
    """Validate table indexes and create missing ones"""
    # 获取数据库中该表的所有索引
    cursor = self.conn.cursor()
    cursor.execute("""
      SELECT name FROM sqlite_master 
      WHERE type='index' 
      AND tbl_name = ?
      AND name NOT LIKE 'sqlite_autoindex_%'  -- 排除自动创建的主键/唯一约束索引
    """, (table.name,))
    existing_indexes = {row[0] for row in cursor.fetchall()}
    
    # 获取表定义中索引的名称
    defined_index_names = {index.name for index in table.indexes}
    
    # 找出缺失的索引
    missing_indexes = defined_index_names - existing_indexes
    
    # 创建缺失的索引
    for index in table.indexes:
      if index.name in missing_indexes:
        self.logger.info(f"Adding index {index.name} on table {table.name}")
        try:
          self.conn.execute(index.GetCreateSQL(table.name))
          self.logger.debug(f"Executed: {index.GetCreateSQL(table.name)}")
        except sqlite3.Error as e:
          self.logger.error(f"Failed to create index {index.name}: {str(e)}")
          # 继续处理其他索引

  def _GetExistingFields(self, table_name: str) -> dict:
    """Get existing table field information"""
    cursor = self.conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return {
      row[1]: {"name": row[1], "type": row[2].upper(), "not_null": row[3]}
      for row in cursor.fetchall()
    }

  def _AddFieldToTable(self, table_name: str, field: SQLField):
    """Add new field to existing table"""
    # 检查SQLite对ALTER TABLE ADD COLUMN的限制
    unsupported_constraints = []
    if field.is_primary:
      unsupported_constraints.append("PRIMARY KEY")
    if field.unique:
      unsupported_constraints.append("UNIQUE")
    if field.check:
      unsupported_constraints.append("CHECK")
    
    if unsupported_constraints:
      raise ValueError(
        f"Cannot add field '{field.name}' with unsupported constraints: "
        f"{', '.join(unsupported_constraints)}. SQLite's ALTER TABLE ADD COLUMN "
        f"only supports NOT NULL and DEFAULT constraints."
      )
    
    # 构建并执行ALTER TABLE语句
    query = f"ALTER TABLE {table_name} ADD COLUMN {field.name} {field.GetCreateStr()}"
    try:
      self.conn.execute(query)
      self.logger.debug(f"Executed: {query}")
    except sqlite3.Error as e:
      self.logger.error(f"Failed to add field {table_name}.{field.name}: {str(e)}")
      raise


  def AddTable(self, table: SQLTable) -> None:
    """Add table to database"""
    if self.conn is None:
      self.logger.warning("[AddTable] Database not connected")
      return
      
    self.structure.AddTable(table)
    self._CreateTable(table)
    self.logger.info(f"Created new table: {table.name}")

  def _CreateTable(self, table: SQLTable):
    """Create table structure (supports full constraints)"""
    try:
      # Create table structure
      self.conn.execute(table.GetCreateTableSQL())
      self.logger.debug(f"Executed: {table.GetCreateTableSQL()}")
      
      # Create indexes
      for index_sql in table.GetCreateIndexSQLs():
        self.conn.execute(index_sql)
        self.logger.debug(f"Executed: {index_sql}")
        
      self.conn.commit()
    except sqlite3.Error as e:
      self.logger.error(f"Failed to create table {table.name}: {str(e)}")
      self.conn.rollback()

  def Commit(self):
    """Manually commit transaction"""
    if self.conn:
      self.conn.commit()
      self.logger.debug("Transaction committed")

  def Rollback(self):
    """Rollback transaction"""
    if self.conn:
      self.conn.rollback()
      self.logger.debug("Transaction rolled back")

  def Close(self):
    """Close database connection"""
    if self.conn:
      if self.commit_when_leave:
        self.Commit()
      self.conn.close()
      self.conn = None
      self.logger.info(f"Database connection closed: {self.path}")

  def Execute(self, sql: str, params: tuple = None, many: bool = False):
    """Execute SQL query"""
    cursor = self.conn.cursor()
    try:
      if many and isinstance(params, list):
        cursor.executemany(sql, params)
      else:
        cursor.execute(sql, params or ())
      return cursor
    except sqlite3.Error as e:
      self.logger.error(f"SQL execution error: {str(e)}, sql: {sql}, params: {params}")
      raise

  def __del__(self):
    self.Close()

if __name__ == "__main__":
  table_name_initiate_dict = {
    "BasicTable": {
      "field_definition": {
        "id": "INTEGER",
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
  conn.Commit()
  pass