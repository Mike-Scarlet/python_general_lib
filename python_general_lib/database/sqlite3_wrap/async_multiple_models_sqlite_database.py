import asyncio
import typing
import logging
from python_general_lib.database.sqlite3_wrap.sqlite_python_class_integration import PySQLModel, Field
from python_general_lib.database.multiple_models_sqlite_database import MultipleModelsSQLiteDatabase
from python_general_lib.async_component.async_timed_trigger import AsyncTimedTrigger
from python_general_lib.interface.json_serializable import IJsonSerializable

class AsyncMultipleModelsSQLiteDatabase:
    def __init__(self, db_path: str, model_classes: typing.List[typing.Type[PySQLModel]], 
                 model_primary_keys_dict: typing.Optional[typing.Dict[typing.Type, typing.Union[str, typing.List[str]]]] = None,
                 class_to_table_name_dict: typing.Optional[typing.Dict[typing.Type, str]] = None) -> None:
        """
        异步SQLite数据库管理器，基于同步版本封装
        
        参数:
            db_path: 数据库文件路径
            model_classes: PySQLModel模型类列表
            model_primary_keys_dict: 模型类的主键字典
            class_to_table_name_dict: 模型类到表名的映射字典
        """
        # 创建同步数据库实例
        self.sync_db = MultipleModelsSQLiteDatabase(
            db_path, model_classes, model_primary_keys_dict, class_to_table_name_dict
        )
        
        # 异步锁和定时触发器
        self._lock = asyncio.Lock()
        self._timed_trigger = AsyncTimedTrigger()
        
        self.logger = logging.getLogger("AsyncMultipleModelsSQLiteDatabase")
        self.logger.setLevel(logging.INFO)
    
    async def Initiate(self, check_same_thread: bool = True, commit_when_leave: bool = False, 
                      verbose_level: int = 10, commit_interval: float = 20.0) -> None:
        """
        初始化数据库连接和结构
        
        参数:
            check_same_thread: SQLite线程安全检查
            commit_when_leave: 退出时自动提交（设置为False，由我们控制提交）
            verbose_level: 日志级别
            commit_interval: 自动提交间隔（秒）
        """
        # 初始化同步数据库
        self.sync_db.Initiate(
            check_same_thread=check_same_thread,
            commit_when_leave=commit_when_leave,
            verbose_level=verbose_level
        )
        
        # 设置自动提交间隔
        self._timed_trigger.SetMustCallCallbackTimeInterval(commit_interval)
        self._timed_trigger.SetCallbackAsyncFunction(self.CommitAsync)
        
        # 启动自动提交任务
        await self._timed_trigger.StartTriggerHandlerTask(asyncio.get_running_loop())
        
        self.logger.info(f"Database initialized at {self.sync_db.db_path}")
    
    async def SetTriggerMustCallBackInterval(self, interval: float):
        """设置自动提交间隔"""
        self._timed_trigger.SetMustCallCallbackTimeInterval(interval)
    
    async def AutoCommitAfter(self, seconds: float):
        """在指定时间后自动提交"""
        await self._timed_trigger.ActivateTimedTrigger(seconds)
    
    async def InsertRecord(self, item: IJsonSerializable, or_condition: str = "") -> None:
        """
        异步插入记录
        
        参数:
            item: 实现了IJsonSerializable接口的对象
            or_condition: 冲突解决策略 (如"OR IGNORE", "OR REPLACE")
        """
        async with self._lock:
            self.sync_db.InsertRecord(item, or_condition)
            await self.AutoCommitAfter(5.0)
    
    async def RemoveRecord(self, item: IJsonSerializable) -> None:
        """
        异步删除记录
        
        参数:
            item: 要删除的对象
        """
        async with self._lock:
            self.sync_db.RemoveRecord(item)
            await self.AutoCommitAfter(5.0)
    
    async def QueryRecords(self, model_class: typing.Type[PySQLModel], 
                         query_condition: typing.Optional[str] = None) -> typing.List[PySQLModel]:
        """
        异步查询记录
        
        参数:
            model_class: 模型类
            query_condition: WHERE条件
        
        返回:
            模型对象列表
        """
        # 查询操作不需要加锁
        return self.sync_db.QueryRecords(model_class, query_condition)
    
    async def QueryRecordsAdvanced(self, model_class: typing.Type[PySQLModel], 
                                 sub_condition: typing.Optional[str] = None) -> typing.List[PySQLModel]:
        """
        高级查询记录
        
        参数:
            model_class: 模型类
            sub_condition: SQL子句 (如"WHERE ... ORDER BY ...")
        
        返回:
            模型对象列表
        """
        # 查询操作不需要加锁
        return self.sync_db.QueryRecordsAdvanced(model_class, sub_condition)
    
    async def QueryRecordsAsJson(self, model_class: typing.Type[PySQLModel], 
                               query_condition: typing.Optional[str] = None) -> typing.List[dict]:
        """
        查询记录并返回JSON格式
        
        参数:
            model_class: 模型类
            query_condition: WHERE条件
        
        返回:
            字典列表
        """
        # 查询操作不需要加锁
        return self.sync_db.QueryRecordsAsJson(model_class, query_condition)
    
    async def RawQueryRecords(self, model_class: typing.Type[PySQLModel], 
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
        # 查询操作不需要加锁
        return self.sync_db.RawQueryRecords(model_class, query_key, query_condition)
    
    async def RawSelectFieldFromTableWithReturnFieldName(self, model_class: typing.Type[PySQLModel], 
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
        # 查询操作不需要加锁
        return self.sync_db.RawSelectFieldFromTableWithReturnFieldName(model_class, fields, sub_condition)
    
    async def RecordFieldChanged(self, item: IJsonSerializable, 
                               update_fields: typing.Union[str, typing.List[str]]) -> None:
        """
        异步更新记录的指定字段
        
        参数:
            item: 要更新的对象
            update_fields: 要更新的字段列表
        """
        async with self._lock:
            self.sync_db.RecordFieldChanged(item, update_fields)
            await self.AutoCommitAfter(5.0)
    
    async def CommitAsync(self, lock: bool = True) -> None:
        """异步提交事务"""
        if lock:
            async with self._lock:
                self.sync_db.Commit()
        else:
            self.sync_db.Commit()
    
    async def RollbackAsync(self) -> None:
        """异步回滚事务"""
        async with self._lock:
            if self.sync_db._conn:
                self.sync_db._conn.Rollback()
    
    async def Close(self) -> None:
        """关闭数据库连接"""
        # 停止自动提交任务
        await self._timed_trigger.StopTriggerHandlerTask()
        
        # 提交未提交的事务
        await self.CommitAsync(lock=False)
        
        # 关闭连接
        self.sync_db._conn.Close()
        self.logger.info("Database connection closed")
    
    # 代理其他可能需要的方法
    @property
    def db_path(self):
        return self.sync_db.db_path
    
    @property
    def model_classes(self):
        return self.sync_db.model_classes
    
    @property
    def model_primary_keys_dict(self):
        return self.sync_db.model_primary_keys_dict
    
    @property
    def class_to_table_name_dict(self):
        return self.sync_db.class_to_table_name_dict

# 测试用例
if __name__ == "__main__":
    import datetime
    import asyncio
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    # 测试模型类
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
    
    async def main():
        # 初始化数据库
        db = AsyncMultipleModelsSQLiteDatabase(
            "test_async.db", 
            [TestClassA, TestClassB],
            model_primary_keys_dict={TestClassA: "id", TestClassB: "id"}
        )
        await db.Initiate(commit_interval=10.0)
        
        # 创建并插入记录
        item_a = TestClassA()
        item_a.name = "Test Item"
        item_a.value = 42.0
        await db.InsertRecord(item_a)
        print(f"Inserted item_a")
        
        item_b = TestClassB()
        item_b.description = "Test Description"
        await db.InsertRecord(item_b)
        print(f"Inserted item_b")
        
        # 查询记录
        items_a = await db.QueryRecords(TestClassA)
        print(f"Found {len(items_a)} items in TestClassA")
        for item in items_a:
            print(f"Item ID: {item.id}, Name: {item.name}, Value: {item.value}")
        
        # 更新记录
        if items_a:
            items_a[0].value = 99.0
            await db.RecordFieldChanged(items_a[0], ["value"])
        
        # 删除记录
        await db.RemoveRecord(item_b)
        
        # 关闭数据库
        await db.Close()
    
    asyncio.run(main())