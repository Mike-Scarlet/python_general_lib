
from python_general_lib.database.single_model_sqlite_database import *
from python_general_lib.interface.json_serializable import IJsonSerializable
import random
import tqdm
import time

class A(IJsonSerializable):
  k: int
  v: int
  t: int
  def __init__(self) -> None:
    self.k = None
    self.v = None
    self.t = None
    self._UseDefaultJsonSerializeMethod()

db = SingleModelSQLiteDatabase("/media/ubuntu/data/[]/test/tt.db", A, None)
db.Initiate()

# for _ in tqdm.tqdm(range(100000)):
#   a = A()
#   a.k = random.randint(0, 100)
#   a.v = random.randint(0, 100000)
#   a.t = random.randint(0, 10000)
#   db.InsertRecord(a, "OR IGNORE")

# db.Commit()

db.op.Execute("CREATE INDEX IF NOT EXISTS k_index ON A (k, t);")
# db.op.Execute("DROP INDEX IF EXISTS k_index;")

def Func0():
  result = db.QueryRecordsAsJson(query_condition="k == {}".format(p))

def Func1():
  result = db.QueryRecordsAsJson(query_condition="k == {} ORDER BY t LIMIT 1000 OFFSET 500".format(p))
  # print("")
  # result1 = db.QueryRecordsAsJson(query_condition="k == {} ORDER BY t LIMIT 4000, 2000".format(p))
  # print(p, len(result))

Func = Func1

beg = time.time()
for p in range(102):
  Func()
print(time.time() - beg)

beg = time.time()
for p in range(102):
  Func()
print(time.time() - beg)

beg = time.time()
for p in range(102):
  Func()
print(time.time() - beg)