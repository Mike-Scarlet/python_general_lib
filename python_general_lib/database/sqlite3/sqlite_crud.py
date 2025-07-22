import logging
from typing import Any, Dict, List, Tuple, Union, Optional
from python_general_lib.database.sqlite3.sqlite_connector import SQLite3Connector

class SQLite3CRUD:
  def __init__(self, sqlite_connector: SQLite3Connector):
    """
    Lightweight and efficient SQLite operations library
    
    Features:
    1. Optimized for maximum performance with no redundant verifications
    2. Full CRUD operation support
    3. Efficient batch operations
    4. Clean and easy-to-use API design
    
    Args:
      sqlite_connector: SQLite connector instance
    """
    self.connector = sqlite_connector
    self.logger = logging.getLogger("SQLiteRepository")
  
  # === Insert Operations ===
  def Insert(
    self, 
    table_name: str, 
    data: Dict[str, Any],
    on_conflict: str = ""
  ) -> Optional[int]:
    """
    Insert a single record
    
    Args:
      table_name: Target table name
      data: Data dictionary to insert {column: value}
      on_conflict: ON CONFLICT strategy (e.g., "OR IGNORE", "OR REPLACE")
    
    Returns:
      Last inserted row ID (if using auto-increment primary key)
    """
    columns = []
    values = []
    placeholders = []
    
    for col, val in data.items():
      columns.append(col)
      values.append(val)
      placeholders.append("?")
    
    sql = f"""
      INSERT {on_conflict} INTO {table_name}
      ({", ".join(columns)})
      VALUES ({", ".join(placeholders)})
    """
    
    try:
      cursor = self.connector.Execute(sql, params=tuple(values))
      return cursor.lastrowid
    except Exception as e:
      self.logger.error(f"Insert failed: {str(e)}")
      return None
  
  def InsertBatch(
    self, 
    table_name: str, 
    records: List[Dict[str, Any]],
    on_conflict: str = "",
    batch_size: int = 1000
  ) -> bool:
    """
    Batch insert multiple records
    
    Args:
      table_name: Target table name
      records: List of records to insert
      on_conflict: ON CONFLICT strategy
      batch_size: Number of records per batch
    
    Returns:
      Operation success status
    """
    if not records:
      return True
      
    # Get all column names
    all_columns = set()
    for record in records:
      all_columns.update(record.keys())
    columns = list(all_columns)
    
    # Prepare batch SQL
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"""
      INSERT {on_conflict} INTO {table_name}
      ({", ".join(columns)})
      VALUES ({placeholders})
    """
    
    # Prepare batch data
    batch_values = []
    for record in records:
      row_values = []
      for col in columns:
        row_values.append(record.get(col))
      batch_values.append(tuple(row_values))
    
    # Batch processing
    success = True
    for i in range(0, len(batch_values), batch_size):
      batch = batch_values[i:i+batch_size]
      try:
        self.connector.Execute(sql, params=batch, many=True)
      except Exception as e:
        self.logger.error(f"Batch insert failed at offset {i}: {str(e)}")
        success = False
    
    return success
  
  # === Update Operations ===
  def Update(
    self, 
    table_name: str, 
    updates: Dict[str, Any],
    where: str,
    params: Tuple = ()
  ) -> int:
    """
    Update records
    
    Args:
      table_name: Target table name
      updates: Fields to update {column: new_value}
      where: WHERE clause (use ? placeholders)
      params: WHERE clause parameter values
    
    Returns:
      Number of affected rows
    """
    update_parts = []
    update_values = []
    
    for col, new_value in updates.items():
      update_parts.append(f"{col} = ?")
      update_values.append(new_value)
    
    sql = f"""
      UPDATE {table_name}
      SET {", ".join(update_parts)}
      WHERE {where}
    """
    
    try:
      cursor = self.connector.Execute(sql, params=tuple(update_values) + params)
      return cursor.rowcount
    except Exception:
      return 0
  
  # === Delete Operations ===
  def Delete(
    self, 
    table_name: str, 
    where: str, 
    params: Tuple = ()
  ) -> int:
    """
    Delete records
    
    Args:
      table_name: Target table name
      where: WHERE clause (use ? placeholders)
      params: WHERE clause parameter values
    
    Returns:
      Number of affected rows
    """
    sql = f"DELETE FROM {table_name} WHERE {where}"
    
    try:
      cursor = self.connector.Execute(sql, params=params)
      return cursor.rowcount
    except Exception:
      return 0
  
  # === Query Operations ===
  def Query(
    self,
    table_name: str,
    columns: Union[str, List[str]] = "*",
    where: Optional[str] = None,
    params: Tuple = (),
    joins: Optional[str] = None,
    group_by: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None
  ) -> List[Dict[str, Any]]:
    """
    Generic query method
    
    Args:
      table_name: Target table name
      columns: Columns to select (*, column name, or list of names)
      where: WHERE clause (use ? placeholders)
      params: WHERE clause parameter values
      joins: JOIN statements
      group_by: GROUP BY statement
      order_by: ORDER BY statement
      limit: Row limit
      offset: Row offset
    
    Returns:
      List of result dictionaries
    """
    if isinstance(columns, list):
      columns_str = ", ".join(columns)
    else:
      columns_str = columns
    
    sql = f"SELECT {columns_str} FROM {table_name}"
    
    if joins:
      sql += f" {joins}"
    if where:
      sql += f" WHERE {where}"
    if group_by:
      sql += f" GROUP BY {group_by}"
    if order_by:
      sql += f" ORDER BY {order_by}"
    if limit is not None:
      sql += f" LIMIT {limit}"
    if offset is not None:
      sql += f" OFFSET {offset}"
    
    try:
      cursor = self.connector.Execute(sql, params=params)
      column_names = [d[0] for d in cursor.description]
      return [dict(zip(column_names, row)) for row in cursor.fetchall()]
    except Exception:
      return []
  
  def QueryOne(
    self,
    table_name: str,
    columns: Union[str, List[str]] = "*",
    where: Optional[str] = None,
    params: Tuple = ()
  ) -> Optional[Dict[str, Any]]:
    """
    Query a single record
    
    Args:
      table_name: Target table name
      columns: Columns to select
      where: WHERE clause
      params: WHERE clause parameter values
    
    Returns:
      Single record dictionary or None
    """
    results = self.Query(table_name, columns, where, params, limit=1)
    return results[0] if results else None
  
  # === Aggregate Operations ===
  def Count(
    self, 
    table_name: str, 
    where: Optional[str] = None, 
    params: Tuple = ()
  ) -> int:
    """
    Count records
    
    Args:
      table_name: Target table name
      where: WHERE clause
      params: WHERE clause parameter values
    
    Returns:
      Number of matching records
    """
    sql = f"SELECT COUNT(*) FROM {table_name}"
    if where:
      sql += f" WHERE {where}"
    
    try:
      cursor = self.connector.Execute(sql, params=params)
      result = cursor.fetchone()
      return result[0] if result else 0
    except Exception:
      return 0
  
  # === Raw SQL Operations ===
  def Execute(
    self, 
    sql: str, 
    params: Union[Tuple, List[Tuple]] = (), 
    many: bool = False
  ) -> bool:
    """
    Execute raw SQL
    
    Args:
      sql: SQL statement
      params: Parameter values (single or batch)
      many: Batch operation mode
    
    Returns:
      Operation success status
    """
    try:
      self.connector.Execute(sql, params=params, many=many)
      return True
    except Exception:
      return False