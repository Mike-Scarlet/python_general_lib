import logging
from typing import Any, Dict, List, Tuple, Union, Optional
from python_general_lib.database.sqlite3.sqlite_connector import SQLite3Connector

class SQLite3CRUD:
  def __init__(self, sqlite_connector: SQLite3Connector):
    """
    精简高效的 SQLite 操作库
    
    特性：
    1. 极致性能优化，无任何冗余验证
    2. 完整的 CRUD 操作支持
    3. 高效的批处理操作
    4. 简洁易用的 API 设计
    
    Args:
      sqlite_connector: SQLite 连接器实例
    """
    self.connector = sqlite_connector
    self.logger = logging.getLogger("SQLiteRepository")
  
  # === 插入操作 ===
  def Insert(
    self, 
    table_name: str, 
    data: Dict[str, Any],
    on_conflict: str = ""
  ) -> Optional[int]:
    """
    插入单条记录
    
    Args:
      table_name: 目标表名
      data: 要插入的数据字典 {列名: 值}
      on_conflict: ON CONFLICT 策略 (如 "OR IGNORE", "OR REPLACE")
    
    Returns:
      最后插入的行 ID (如果有自增主键)
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
    批量插入多条记录
    
    Args:
      table_name: 目标表名
      records: 要插入的记录列表
      on_conflict: ON CONFLICT 策略
      batch_size: 每批次处理的记录数量
    
    Returns:
      操作是否成功
    """
    if not records:
      return True
      
    # 获取所有列名
    all_columns = set()
    for record in records:
      all_columns.update(record.keys())
    columns = list(all_columns)
    
    # 准备批处理 SQL
    placeholders = ", ".join(["?"] * len(columns))
    sql = f"""
      INSERT {on_conflict} INTO {table_name}
      ({", ".join(columns)})
      VALUES ({placeholders})
    """
    
    # 准备批处理数据
    batch_values = []
    for record in records:
      row_values = []
      for col in columns:
        row_values.append(record.get(col))
      batch_values.append(tuple(row_values))
    
    # 分批次插入
    success = True
    for i in range(0, len(batch_values), batch_size):
      batch = batch_values[i:i+batch_size]
      try:
        self.connector.Execute(sql, params=batch, many=True)
      except Exception as e:
        self.logger.error(f"Batch insert failed at offset {i}: {str(e)}")
        success = False
    
    return success
  
  # === 更新操作 ===
  def Update(
    self, 
    table_name: str, 
    updates: Dict[str, Any],
    where: str,
    params: Tuple = ()
  ) -> int:
    """
    更新记录
    
    Args:
      table_name: 目标表名
      updates: 要更新的字段 {列名: 新值}
      where: WHERE 条件语句 (使用 ? 占位符)
      params: WHERE 条件参数值
    
    Returns:
      受影响的行数
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
  
  # === 删除操作 ===
  def Delete(
    self, 
    table_name: str, 
    where: str, 
    params: Tuple = ()
  ) -> int:
    """
    删除记录
    
    Args:
      table_name: 目标表名
      where: WHERE 条件语句 (使用 ? 占位符)
      params: WHERE 条件参数值
    
    Returns:
      受影响的行数
    """
    sql = f"DELETE FROM {table_name} WHERE {where}"
    
    try:
      cursor = self.connector.Execute(sql, params=params)
      return cursor.rowcount
    except Exception:
      return 0
  
  # === 查询操作 ===
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
    通用查询方法
    
    Args:
      table_name: 目标表名
      columns: 要查询的列 (可以是 *、单个列名或列名列表)
      where: WHERE 条件语句 (使用 ? 占位符)
      params: WHERE 条件参数值
      joins: JOIN 语句
      group_by: GROUP BY 语句
      order_by: ORDER BY 语句
      limit: 限制返回行数
      offset: 偏移量
    
    Returns:
      查询结果字典列表
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
    查询单条记录
    
    Args:
      table_name: 目标表名
      columns: 要查询的列
      where: WHERE 条件语句
      params: WHERE 条件参数值
    
    Returns:
      单条记录字典 (如果没有记录则返回 None)
    """
    results = self.Query(table_name, columns, where, params, limit=1)
    return results[0] if results else None
  
  # === 聚合操作 ===
  def Count(
    self, 
    table_name: str, 
    where: Optional[str] = None, 
    params: Tuple = ()
  ) -> int:
    """
    获取记录数量
    
    Args:
      table_name: 目标表名
      where: WHERE 条件语句
      params: WHERE 条件参数值
    
    Returns:
      符合条件的记录数量
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
  
  # === 原始 SQL 操作 ===
  def Execute(
    self, 
    sql: str, 
    params: Union[Tuple, List[Tuple]] = (), 
    many: bool = False
  ) -> bool:
    """
    执行原始 SQL 语句
    
    Args:
      sql: SQL 语句
      params: 参数值 (单条或批处理)
      many: 是否为批处理模式
    
    Returns:
      操作是否成功
    """
    try:
      self.connector.Execute(sql, params=params, many=many)
      return True
    except Exception:
      return False