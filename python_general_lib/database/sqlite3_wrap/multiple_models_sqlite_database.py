from sqlite3 import sqlite_version_info
from python_general_lib.database.sqlite3_wrap.sqlite_python_class_integration import PySQLModel, GenerateSQLDatabase, Field
from python_general_lib.database.sqlite3_wrap.sqlite_connector import SQLite3Connector
from python_general_lib.database.sqlite3_wrap.sqlite_crud import SQLite3CRUD
from python_general_lib.interface.json_serializable import IJsonSerializable
import typing
import logging

class MultipleModelsSQLiteDatabase:
    def __init__(self, db_path: str, model_classes: typing.List[typing.Type[PySQLModel]], 
                 class_to_table_name_dict: typing.Optional[typing.Dict[typing.Type, str]] = None) -> None:
        """
        SQLite数据库管理器，支持多个PySQLModel模型类
        
        参数:
            db_path: 数据库文件路径
            model_classes: PySQLModel模型类列表
            class_to_table_name_dict: 可选，模型类到表名的映射字典
        """
        self.db_path = db_path
        self.model_classes = model_classes
        self.class_to_table_name_dict = class_to_table_name_dict or {}
        self._primary_key_cache = {}
        
        # 设置默认表名（如果未提供）
        for model_class in self.model_classes:
            if model_class not in self.class_to_table_name_dict:
                self.class_to_table_name_dict[model_class] = model_class._sql_meta['table_name']
        
        self._connector = None
        self._crud = None
        self.logger = logging.getLogger("MultipleModelsSQLiteDatabase")
        self.logger.setLevel(logging.INFO)
    
    def Initiate(self, check_same_thread: bool = True, commit_when_leave: bool = True, 
                 verbose_level: int = logging.INFO) -> None:
        """
        初始化数据库连接和结构
        
        参数:
            check_same_thread: SQLite线程安全检查
            commit_when_leave: 退出时自动提交
            verbose_level: 日志级别
        """
        # 生成数据库结构
        db_structure = GenerateSQLDatabase(*self.model_classes)
        
        # 初始化连接器
        self._connector = SQLite3Connector(
            self.db_path,
            structure=db_structure,
            commit_when_leave=commit_when_leave,
            verbose_level=verbose_level
        )
        self._connector.Connect(check_same_thread=check_same_thread)
        self._connector.TableValidation()
        
        # 初始化CRUD操作
        self._crud = SQLite3CRUD(self._connector)
        self.logger.info(f"Database initialized at {self.db_path} with SQLite version {sqlite_version_info}")
    
    def InsertRecord(self, item: IJsonSerializable, on_conflict: str = "", update_primary_key: bool = False) -> typing.Optional[int]:
        """
        插入记录
        
        参数:
            item: 实现了IJsonSerializable接口的对象
            on_conflict: 冲突解决策略 (如"OR IGNORE", "OR REPLACE")
        
        返回:
            插入的行ID (如果使用自增主键)
        """
        table_name = self.class_to_table_name_dict[type(item)]
        data = item.ToJson()
        row_id = self._crud.Insert(table_name, data, on_conflict)
        
        # 更新自增ID到对象
        if update_primary_key and row_id is not None:
            cls = type(item)
            primary_keys = self._get_primary_keys(cls)
            
            # 如果只有一个自增主键，则更新ID
            if len(primary_keys) == 1:
                pk_field = primary_keys[0]
                field_def = getattr(cls, pk_field, None)
                
                # 检查是否是自增字段
                if isinstance(field_def, Field) and field_def.primary_key:
                    # 获取字段类型
                    annotations = cls.__annotations__
                    field_type = annotations.get(pk_field, None)
                    
                    # 只更新整数类型的主键
                    if field_type in (int, 'INTEGER', 'INT'):
                        setattr(item, pk_field, row_id)
        
        return row_id
    
    def InsertBatch(self, items: typing.List[IJsonSerializable], on_conflict: str = "", 
                    batch_size: int = 1000) -> bool:
        """
        批量插入记录
        
        参数:
            items: 对象列表
            on_conflict: 冲突解决策略
            batch_size: 每批插入数量
        
        返回:
            操作是否成功
        """
        if not items:
            return True
        
        # 按表分组
        grouped_items = {}
        for item in items:
            cls = type(item)
            table_name = self.class_to_table_name_dict[cls]
            if table_name not in grouped_items:
                grouped_items[table_name] = []
            grouped_items[table_name].append(item.ToJson())
        
        # 批量插入每个表
        success = True
        for table_name, records in grouped_items.items():
            if not self._crud.InsertBatch(table_name, records, on_conflict, batch_size):
                success = False
                self.logger.error(f"Batch insert failed for table {table_name}")
        
        # 批量插入后无法获取每个对象的ID，需要单独查询更新
        # 对于自增ID，批量插入后需要单独更新对象
        # 这里不自动更新，需要调用方手动处理
        
        return success
    
    def RemoveRecord(self, item: IJsonSerializable) -> int:
        """
        删除记录
        
        参数:
            item: 要删除的对象
        
        返回:
            删除的行数
        """
        cls = type(item)
        table_name = self.class_to_table_name_dict[cls]
        primary_keys = self._get_primary_keys(cls)
        
        if not primary_keys:
            raise ValueError(f"Model class {cls.__name__} has no primary key defined")
        
        # 构建WHERE条件
        where_parts = []
        params = []
        for pk in primary_keys:
            value = getattr(item, pk)
            where_parts.append(f"{pk} = ?")
            params.append(value)
        
        where_stmt = " AND ".join(where_parts)
        return self._crud.Delete(table_name, where_stmt, tuple(params))
    
    def QueryRecords(self, model_class: typing.Type[PySQLModel], 
                     where: typing.Optional[str] = None, 
                     params: typing.Tuple = ()) -> typing.List[PySQLModel]:
        """
        查询记录
        
        参数:
            model_class: 模型类
            where: WHERE条件 (使用?占位符)
            params: WHERE条件参数
        
        返回:
            模型对象列表
        """
        table_name = self.class_to_table_name_dict[model_class]
        records = self._crud.Query(table_name, where=where, params=params)
        return [self._record_to_model(model_class, record) for record in records]
    
    def QueryOne(self, model_class: typing.Type[PySQLModel], 
                 where: typing.Optional[str] = None, 
                 params: typing.Tuple = ()) -> typing.Optional[PySQLModel]:
        """
        查询单个记录
        
        参数:
            model_class: 模型类
            where: WHERE条件 (使用?占位符)
            params: WHERE条件参数
        
        返回:
            模型对象或None
        """
        table_name = self.class_to_table_name_dict[model_class]
        record = self._crud.QueryOne(table_name, where=where, params=params)
        return self._record_to_model(model_class, record) if record else None
    
    def QueryRecordsAdvanced(self, model_class: typing.Type[PySQLModel], 
                             sub_condition: typing.Optional[str] = None) -> typing.List[PySQLModel]:
        """
        高级查询 (支持完整的SQL子句)
        
        参数:
            model_class: 模型类
            sub_condition: SQL子句 (如"WHERE ... ORDER BY ...")
        
        返回:
            模型对象列表
        """
        table_name = self.class_to_table_name_dict[model_class]
        sql = f"SELECT * FROM {table_name}"
        if sub_condition:
            sql += " " + sub_condition
        
        cursor = self._connector.conn.cursor()
        cursor.execute(sql)
        column_names = [d[0] for d in cursor.description]
        records = [dict(zip(column_names, row)) for row in cursor.fetchall()]
        return [self._record_to_model(model_class, record) for record in records]
    
    def QueryRecordsAsJson(self, model_class: typing.Type[PySQLModel], 
                           where: typing.Optional[str] = None, 
                           params: typing.Tuple = ()) -> typing.List[dict]:
        """
        查询记录并返回JSON格式
        
        参数:
            model_class: 模型类
            where: WHERE条件 (使用?占位符)
            params: WHERE条件参数
        
        返回:
            字典列表
        """
        table_name = self.class_to_table_name_dict[model_class]
        return self._crud.Query(table_name, where=where, params=params)
    
    def RawQueryRecords(self, model_class: typing.Type[PySQLModel], 
                        query_key: str = "*", 
                        query_condition: typing.Optional[str] = None) -> typing.List[typing.Any]:
        """
        原始查询记录
        
        参数:
            model_class: 模型类
            query_key: 查询字段
            query_condition: 查询条件
        
        返回:
            原始记录列表
        """
        table_name = self.class_to_table_name_dict[model_class]
        where = query_condition if query_condition else None
        return self._crud.Query(table_name, columns=query_key, where=where)
    
    def RawSelectFieldFromTableWithReturnFieldName(self, model_class: typing.Type[PySQLModel], 
                                                   fields: typing.Union[str, typing.List[str]], 
                                                   sub_condition: typing.Optional[str] = None) -> typing.List[dict]:
        """
        查询指定字段并返回字段名
        
        参数:
            model_class: 模型类
            fields: 字段列表
            sub_condition: SQL子句
        
        返回:
            包含字段名的字典列表
        """
        table_name = self.class_to_table_name_dict[model_class]
        columns = fields if isinstance(fields, list) else [fields]
        sql = f"SELECT {', '.join(columns)} FROM {table_name}"
        if sub_condition:
            sql += " " + sub_condition
        
        cursor = self._connector.conn.cursor()
        cursor.execute(sql)
        column_names = [d[0] for d in cursor.description]
        return [dict(zip(column_names, row)) for row in cursor.fetchall()]
    
    def RecordFieldChanged(self, item: IJsonSerializable, 
                           update_fields: typing.Union[str, typing.List[str]]) -> int:
        """
        更新记录的指定字段
        
        参数:
            item: 要更新的对象
            update_fields: 要更新的字段列表
        
        返回:
            更新的行数
        """
        cls = type(item)
        table_name = self.class_to_table_name_dict[cls]
        primary_keys = self._get_primary_keys(cls)
        
        if not primary_keys:
            raise ValueError(f"Model class {cls.__name__} has no primary key defined")
        
        if isinstance(update_fields, str):
            update_fields = [update_fields]
        
        # 构建更新字典
        updates = {field: getattr(item, field) for field in update_fields}
        
        # 构建WHERE条件
        where_parts = []
        params = []
        for pk in primary_keys:
            value = getattr(item, pk)
            where_parts.append(f"{pk} = ?")
            params.append(value)
        
        where_stmt = " AND ".join(where_parts)
        return self._crud.Update(table_name, updates, where_stmt, tuple(params))
    
    def Commit(self) -> None:
        """提交事务"""
        self._connector.Commit()
        self.logger.debug("Transaction committed")
    
    def Close(self) -> None:
        """关闭数据库连接"""
        if self._connector:
            self._connector.Close()
            self.logger.info("Database connection closed")
    
    def _get_primary_keys(self, model_class: typing.Type[PySQLModel]) -> typing.List[str]:
        """获取模型类的主键字段"""
        if model_class in self._primary_key_cache:
            return self._primary_key_cache[model_class]
        
        meta = model_class._sql_meta
        primary_key = meta.get('primary_key')
        
        if primary_key:
            if isinstance(primary_key, str):
                result = [primary_key]
            else:
                result = primary_key
        else:
            # 检查字段级主键定义
            result = []
            for field_name, field_def in model_class.__dict__.items():
                if isinstance(field_def, Field) and field_def.primary_key:
                    result.append(field_name)
        
        # 将结果存入缓存
        self._primary_key_cache[model_class] = result
        return result
    
    def _record_to_model(self, model_class: typing.Type[PySQLModel], record: dict) -> PySQLModel:
        """将数据库记录转换为模型对象"""
        obj = model_class()
        obj.FromJson(record)
        return obj

if __name__ == "__main__":
    import datetime
    # 测试示例
    @PySQLModel(initialize_fields=True)
    class TestClassA:
        id: int = Field(primary_key=True)
        name: str = Field(not_null=True)
        value: float = Field(default=0.0)
    
    @PySQLModel
    class TestClassB:
        id: int = Field(primary_key=True)
        description: str = Field(not_null=True)
        timestamp: datetime.datetime = Field(default="CURRENT_TIMESTAMP")
    
    # 初始化数据库
    db = MultipleModelsSQLiteDatabase("test.db", [TestClassA, TestClassB])
    db.Initiate()
    
    # 创建并插入记录
    item_a = TestClassA()
    item_a.name = "Test Item"
    item_a.value = 42.0
    inserted_id = db.InsertRecord(item_a, update_primary_key=True)
    print(f"Inserted item_a with id: {item_a.id}")  # 现在应该打印出非零ID
    
    item_b = TestClassB()
    item_b.description = "Test Description"
    inserted_id = db.InsertRecord(item_b, update_primary_key=True)
    print(f"Inserted item_b with id: {item_b.id}")  # 现在应该打印出非零ID
    
    # 查询记录
    items_a = db.QueryRecords(TestClassA)
    print(f"Found {len(items_a)} items in TestClassA")
    for item in items_a:
        print(f"Item ID: {item.id}, Name: {item.name}, Value: {item.value}")
    
    # 更新记录
    if items_a:
        items_a[0].value = 99.0
        db.RecordFieldChanged(items_a[0], ["value"])
    
    # 删除记录
    db.RemoveRecord(item_b)
    
    # 关闭数据库
    db.Close()