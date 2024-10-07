
from SQLiteWrapper import *
from python_general_lib.interface.json_serializable import *
import typing

def _ItemToWhereStatement(item):
  if isinstance(item, str):
    return "'{}'".format(item)
  return item

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

  def Initiate(self, check_same_thread=True, commit_when_leave=True, verbose_level=10):
    # initiate by model class
    # create a temp model object
    db_initiate_dict = ClassAndPrimaryKeyToTableInitiateDict(self.model_class, self.table_name, self.primary_keys)
    db_table_structure = SQLDatabase.CreateFromDict(db_initiate_dict)

    # then connect the db
    self._conn = SQLite3Connector(self.db_path, db_table_structure, 
                                  commit_when_leave=commit_when_leave,
                                  verbose_level=verbose_level)
    self._conn.Connect(do_check=False, check_same_thread=check_same_thread)
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
  
  def QueryRecordsAdvanced(self, sub_condition: str=None):
    raw_records = self._op.SelectFieldFromTableAdvanced("*", self.table_name, sub_condition)
    results = []
    for record in raw_records:
      item = self.model_class()
      item.FromJson(record)
      results.append(item)
    return results

  def QueryRecordsAsJson(self, query_condition: str=None):
    raw_records = self._op.SelectFieldFromTable("*", self.table_name, query_condition)
    results = []
    for record in raw_records:
      results.append(record)
    return results
  
  def RawQueryRecords(self, query_key="*", query_condition: str=None):
    raw_records = self._op.RawSelectFieldFromTable(query_key, self.table_name, query_condition)
    results = []
    for record in raw_records:
      results.append(record)
    return results
  
  def RawSelectFieldFromTableWithReturnFieldName(self, fields, sub_condition: str=None):
    """ fast interface """
    raw_records = self._op.RawSelectFieldFromTableWithReturnFieldName(fields, self.table_name, sub_condition)
    return raw_records
  
  def RecordFieldChanged(self, item: IJsonSerializable, update_fields: typing.Union[str, typing.List[str]]):
    if self.primary_keys is None or len(self.primary_keys) == 0:
      raise ValueError("RecordFieldChanged only available for class with primary_keys")
    if isinstance(update_fields, str):
      update_fields = [update_fields]
    if len(update_fields) == 0:
      return
    update_dict = {}
    for field_name in update_fields:
      update_dict[field_name] = getattr(item, field_name)
    
    # primary key to where
    where_syntaxs = []
    for key in self.primary_keys:
      where_syntaxs.append("{} = {}".format(key, _ItemToWhereStatement(getattr(item, key))))
    where_stmt = " AND ".join(where_syntaxs)
    self._op.UpdateFieldFromTable(
      update_dict, 
      self.table_name, 
      where_stmt)

  def Commit(self):
    self._op.Commit()

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