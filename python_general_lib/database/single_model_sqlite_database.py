
from SQLiteWrapper import *
from python_general_lib.interface.json_serializable import *

class SingleModelSQLiteDatabase:
  def __init__(self, db_path, model_class, primary_keys) -> None:
    self.db_path = db_path
    self.model_class = model_class
    self.primary_keys = primary_keys

    self.conn = None
    self.op = None

  def Initiate(self):
    # initiate by model class
    # create a temp model object
    table_name = self.model_class.__name__
    db_initiate_dict = {table_name: {"field_definition": {}}}
    temp_obj = self.model_class()
    for name, python_type in self.model_class.__annotations__.items():
      field_str = SQLField.GetSQLTypeFromPythonType(python_type)
      if isinstance(getattr(temp_obj, name), python_type) and python_type != str:   # string is not enabled
        field_str += " DEFAULT {}".format(getattr(temp_obj, name))
      db_initiate_dict[table_name]["field_definition"][name] = field_str
    db_initiate_dict[table_name]["primary_keys"] = self.primary_keys
    # print(json.dumps(db_initiate_dict, indent=2))

    db_table_structure = SQLDatabase.CreateFromDict(db_initiate_dict)

    # then connect the db
    self.conn = SQLite3Connector(self.db_path, db_table_structure)
    self.conn.Connect(do_check=False)
    self.conn.TableValidation()

    self.op = SQLite3Operator(self.conn)

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