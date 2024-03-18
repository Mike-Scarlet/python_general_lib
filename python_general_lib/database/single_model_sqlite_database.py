
from SQLiteWrapper import *
from python_general_lib.interface.json_serializable import *
import typing

class SingleModelSQLiteDatabase:
  model_class: typing.ClassVar[IJsonSerializable]
  def __init__(self, db_path: str, model_class, primary_keys: typing.Union[str, typing.List[str]]) -> None:
    self.db_path = db_path
    self.model_class = model_class
    self.table_name = model_class.__name__
    self.primary_keys = primary_keys
    if isinstance(self.primary_keys, str):
      self.primary_keys = [self.primary_keys]

    self._conn = None
    self._op = None

  def Initiate(self):
    # initiate by model class
    # create a temp model object
    db_initiate_dict = ClassAndPrimaryKeyToTableInitiateDict(self.model_class, self.table_name, self.primary_keys)
    db_table_structure = SQLDatabase.CreateFromDict(db_initiate_dict)

    # then connect the db
    self._conn = SQLite3Connector(self.db_path, db_table_structure)
    self._conn.Connect(do_check=False)
    self._conn.TableValidation()

    self._op = SQLite3Operator(self._conn)

  def InsertRecord(self, item: IJsonSerializable, or_condition: str=""):
    insert_dict = item.ToJson()
    self._op.InsertDictToTable(insert_dict, self.table_name, or_condition)

  def QueryRecords(self, query_condition: str=None):
    raw_records = self._op.SelectFieldFromTable("*", self.table_name, query_condition)
    results = []
    for record in raw_records:
      item = self.model_class()
      item.FromJson(record)
      results.append(item)
    return results

  def QueryRecordsAsJson(self, query_condition: str=None):
    raw_records = self.op.SelectFieldFromTable("*", self.table_name, query_condition)
    results = []
    for record in raw_records:
      results.append(record)
    return results
  
  def RawQueryRecords(self, query_key="*", query_condition: str=None):
    raw_records = self.op.RawSelectFieldFromTable(query_key, self.table_name, query_condition)
    results = []
    for record in raw_records:
      results.append(record)
    return results

  def Commit(self):
    self.op.Commit()

if __name__ == "__main__":
  class TestClass:
    a: int
    b: str
    def __init__(self) -> None:
      self.a = 0
      self.b = None
  
  # annotations = TestClass.__annotations__

  smsd = SingleModelSQLiteDatabase("", TestClass, "a")
  smsd.Initiate()
  pass