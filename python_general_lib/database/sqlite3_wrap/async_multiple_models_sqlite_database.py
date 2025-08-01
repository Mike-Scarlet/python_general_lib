import asyncio
import typing
import logging
from python_general_lib.database.sqlite3_wrap.sqlite_python_class_integration import PySQLModel, Field
from python_general_lib.database.sqlite3_wrap.multiple_models_sqlite_database import MultipleModelsSQLiteDatabase
from python_general_lib.async_component.async_timed_trigger import AsyncTimedTrigger
from python_general_lib.interface.json_serializable import IJsonSerializable

Tp = typing.TypeVar('Tp')

class AsyncMultipleModelsSQLiteDatabase:
    def __init__(self, db_path: str, model_classes: typing.List[typing.Type[Tp]], 
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
            db_path, model_classes, class_to_table_name_dict
        )
        
        # 异步锁和定时触发器
        self._lock = asyncio.Lock()
        self._timed_trigger = AsyncTimedTrigger()
        
        self.logger = logging.getLogger("AsyncMultipleModelsSQLiteDatabase")
        self.logger.setLevel(logging.INFO)
    
    async def Initiate(self, loop: asyncio.AbstractEventLoop, check_same_thread: bool = True, commit_when_leave: bool = False, 
                      verbose_level: int = logging.INFO, commit_interval: float = 20.0) -> None:
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
        await self._timed_trigger.StartTriggerHandlerTask(loop)
        
        self.logger.info(f"Database initialized at {self.sync_db.db_path}")
    
    async def SetTriggerMustCallBackInterval(self, interval: float):
        """设置自动提交间隔"""
        self._timed_trigger.SetMustCallCallbackTimeInterval(interval)
    
    async def AutoCommitAfter(self, seconds: float):
        """在指定时间后自动提交"""
        await self._timed_trigger.ActivateTimedTrigger(seconds)
    
    async def InsertRecord(self, item: IJsonSerializable, on_conflict: str = "", update_primary_key: bool = False) -> None:
        """
        异步插入记录
        
        参数:
            item: 实现了IJsonSerializable接口的对象
            or_condition: 冲突解决策略 (如"OR IGNORE", "OR REPLACE")
        """
        async with self._lock:
            self.sync_db.InsertRecord(item, on_conflict, update_primary_key)
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
    
    async def QueryRecords(self, model_class: typing.Type[Tp], 
                           where: typing.Optional[str] = None, 
                           params: typing.Tuple = ()) -> typing.List[Tp]:
        """
        异步查询记录
        
        参数:
            model_class: 模型类
        
        返回:
            模型对象列表
        """
        # 查询操作不需要加锁
        return self.sync_db.QueryRecords(model_class, where, params)
    
    async def QueryOne(self, model_class: typing.Type[Tp], 
                       where: typing.Optional[str] = None, 
                       params: typing.Tuple = ()) -> typing.Optional[Tp]:
        """异步查询单条记录"""
        return self.sync_db.QueryOne(model_class, where, params)

    
    async def QueryRecordsAdvanced(self, model_class: typing.Type[Tp], 
                                   sub_condition: typing.Optional[str] = None) -> typing.List[Tp]:
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
    
    async def QueryRecordsAsJson(self, model_class: typing.Type[Tp], 
                                 where: typing.Optional[str] = None, 
                                 params: typing.Tuple = ()) -> typing.List[dict]:
        """
        查询记录并返回JSON格式
        
        参数:
            model_class: 模型类
            query_condition: WHERE条件
        
        返回:
            字典列表
        """
        # 查询操作不需要加锁
        return self.sync_db.QueryRecordsAsJson(model_class, where, params)
    
    async def RawQueryRecords(self, model_class: typing.Type[Tp], 
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
    
    async def RawSelectFieldFromTableWithReturnFieldName(self, model_class: typing.Type[Tp], 
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
    
    async def Close(self) -> None:
        """关闭数据库连接"""
        # 停止自动提交任务
        await self._timed_trigger.StopTriggerHandlerTask()
        
        # 提交未提交的事务
        await self.CommitAsync(lock=False)
        
        # 关闭连接
        self.sync_db.Close()
        self.logger.info("Database connection closed")

    async def StopTrigger(self):
        await self.CommitAsync()
        await self._timed_trigger.StopTriggerHandlerTask()

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
        # 初始化数据库 (修正参数)
        db = AsyncMultipleModelsSQLiteDatabase(
            "test_async.db", 
            [TestClassA, TestClassB]
        )
        await db.Initiate(commit_interval=10.0)
        
        # 创建并插入记录
        item_a = TestClassA()
        item_a.name = "Test Item"
        item_a.value = 42.0
        await db.InsertRecord(item_a, update_primary_key=True)  # 使用默认的update_primary_key=True
        print(f"Inserted item_a with ID: {item_a.id}")  # 检查ID更新
        
        item_b = TestClassB()
        item_b.description = "Test Description"
        await db.InsertRecord(item_b, update_primary_key=True)
        print(f"Inserted item_b with ID: {item_b.id}")
        
        # 使用QueryOne方法
        first_a = await db.QueryOne(TestClassA, where="name = ?", params=("Test Item",))
        if first_a:
            print(f"First item value: {first_a.value}")
        
        # 更新记录
        if first_a:
            first_a.value = 99.0
            await db.RecordFieldChanged(first_a, ["value"])
        
        # 删除记录
        await db.RemoveRecord(item_b)
        
        # 查询TestClassB检查是否删除成功
        items_b = await db.QueryRecords(TestClassB)
        print(f"Items in TestClassB after deletion: {len(items_b)}")
        
        # 关闭数据库
        await db.Close()
    
    asyncio.run(main())