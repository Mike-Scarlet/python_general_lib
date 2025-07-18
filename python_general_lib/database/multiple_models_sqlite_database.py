
from SQLiteWrapper import *
from python_general_lib.interface.json_serializable import *
import typing

def _ItemToWhereStatement(item):
  if isinstance(item, str):
    return "'{}'".format(item)
  return item

class MultipleModelsSQLiteDatabase:
  def __init__(self, db_path: str, model_classes, model_primary_keys_dict=None, class_to_table_name_dict=None) -> None:
    # primary_keys: typing.Union[str, typing.List[str]]
    self.db_path = db_path
    self.model_classes = model_classes
    self.class_to_table_name_dict = class_to_table_name_dict
    self.model_primary_keys_dict = model_primary_keys_dict
    if self.model_primary_keys_dict is None:
      self.model_primary_keys_dict = {}
    if self.class_to_table_name_dict is None:
      self.class_to_table_name_dict = {}
    for model_class in self.model_classes:
      if model_class not in self.class_to_table_name_dict:
        self.class_to_table_name_dict[model_class] = model_class.__name__

    self._conn = None
    self._op = None

  def Initiate(self, check_same_thread=True, commit_when_leave=True, verbose_level=10):
    # initiate by model class
    # create a temp model object
    db_initiate_dict_all = {}
    for model_class in self.model_classes:
      model_primary_keys = self.model_primary_keys_dict.get(model_class, None)
      if isinstance(model_primary_keys, str):
        model_primary_keys = [model_primary_keys]
      iter_initiate_dict = ClassAndPrimaryKeyToTableInitiateDict(
        model_class, self.class_to_table_name_dict[model_class], model_primary_keys)
      for k, v in iter_initiate_dict.items():
        db_initiate_dict_all[k] = v
    db_table_structure = SQLDatabase.CreateFromDict(db_initiate_dict_all)

    # then connect the db
    self._conn = SQLite3Connector(self.db_path, db_table_structure, 
                                  commit_when_leave=commit_when_leave,
                                  verbose_level=verbose_level)
    self._conn.Connect(do_check=False, check_same_thread=check_same_thread)
    self._conn.TableValidation()

    self._op = SQLite3Operator(self._conn)

  def InsertRecord(self, item: IJsonSerializable, or_condition: str=""):
    table_name = self.class_to_table_name_dict[item.__class__]
    insert_dict = item.ToJson()
    self._op.InsertDictToTable(insert_dict, table_name, or_condition)

  def RemoveRecord(self, item: IJsonSerializable):
    table_name = self.class_to_table_name_dict[item.__class__]
    item_primary_keys = self.model_primary_keys_dict.get(item.__class__, None)

    if item_primary_keys is None or len(item_primary_keys) == 0:
      raise ValueError("RemoveRecord only available for class with primary_keys")
    # primary key to where
    where_syntaxs = []
    for key in item_primary_keys:
      where_syntaxs.append("{} = {}".format(key, _ItemToWhereStatement(getattr(item, key))))
    where_stmt = " AND ".join(where_syntaxs)
    self._op.DeleteFromTableByCondition(table_name, where_stmt)

  def QueryRecords(self, model_class, query_condition: str=None):
    table_name = self.class_to_table_name_dict[model_class]
    raw_records = self._op.SelectFieldFromTable("*", table_name, query_condition)
    results = []
    for record in raw_records:
      item = model_class()
      item.FromJson(record)
      results.append(item)
    return results
  
  def QueryRecordsAdvanced(self, model_class, sub_condition: str=None):
    table_name = self.class_to_table_name_dict[model_class]
    raw_records = self._op.SelectFieldFromTableAdvanced("*", table_name, sub_condition)
    results = []
    for record in raw_records:
      item = model_class()
      item.FromJson(record)
      results.append(item)
    return results

  def QueryRecordsAsJson(self, model_class, query_condition: str=None):
    table_name = self.class_to_table_name_dict[model_class]
    raw_records = self._op.SelectFieldFromTable("*", table_name, query_condition)
    results = []
    for record in raw_records:
      results.append(record)
    return results
  
  def RawQueryRecords(self, model_class, query_key="*", query_condition: str=None):
    table_name = self.class_to_table_name_dict[model_class]
    raw_records = self._op.RawSelectFieldFromTable(query_key, table_name, query_condition)
    results = []
    for record in raw_records:
      results.append(record)
    return results
  
  def RawSelectFieldFromTableWithReturnFieldName(self, model_class, fields, sub_condition: str=None):
    """ fast interface """
    table_name = self.class_to_table_name_dict[model_class]
    raw_records = self._op.RawSelectFieldFromTableWithReturnFieldName(fields, table_name, sub_condition)
    return raw_records
  
  def RecordFieldChanged(self, item: IJsonSerializable, update_fields: typing.Union[str, typing.List[str]]):
    item_primary_keys = self.model_primary_keys_dict.get(item.__class__, None)
    if item_primary_keys is None or len(item_primary_keys) == 0:
      raise ValueError(f"RecordFieldChanged only available for class with primary_keys, class: {item.__class__}")
    if isinstance(update_fields, str):
      update_fields = [update_fields]
    if len(update_fields) == 0:
      return
    update_dict = {}
    for field_name in update_fields:
      update_dict[field_name] = getattr(item, field_name)
    
    # primary key to where
    table_name = self.class_to_table_name_dict[item.__class__]
    where_syntaxs = []
    for key in item_primary_keys:
      where_syntaxs.append("{} = {}".format(key, _ItemToWhereStatement(getattr(item, key))))
    where_stmt = " AND ".join(where_syntaxs)
    self._op.UpdateFieldFromTable(update_dict, table_name, where_stmt)

  def Commit(self):
    self._op.Commit()

if __name__ == "__main__":
  class TestClassA:
    a: int
    b: str
    def __init__(self) -> None:
      self.a = 0
      self.b = None
      
  class TestClassB:
    a: int
    b: str
    def __init__(self) -> None:
      self.a = 0
      self.b = None
  
  # annotations = TestClass.__annotations__

  smsd = MultipleModelsSQLiteDatabase("test.db", [TestClassA, TestClassB])
  smsd.Initiate()
  pass