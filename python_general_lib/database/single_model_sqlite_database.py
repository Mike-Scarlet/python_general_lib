
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

    pass

if __name__ == "__main__":
  class TestClass:
    a: int
    def __init__(self) -> None:
      self.a = None
  
  annotations = TestClass.__annotations__
  pass