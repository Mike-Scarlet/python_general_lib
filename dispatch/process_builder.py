
__all__ = [
  "ThreadQueue",
  "OneInProcessPool"
]

import multiprocessing

ThreadQueue = multiprocessing.Queue

def OneInThreadProcessWrapper(i: int, in_queue: multiprocessing.Queue, process_fn, init_obj_fn):
  obj = None
  if init_obj_fn is not None:
    obj = init_obj_fn(i=i)
  while True:
    item = in_queue.get()
    if item is None:
      break
    process_fn(item=item, i=i, obj=obj)

class OneInProcessPool:
  def __init__(self, thread_count, in_queue: multiprocessing.Queue, process_fn, init_obj_fn=None) -> None:
    self.__in_queue = in_queue
    self.__processes = []
    self.__thread_count = thread_count
    self.__active = True
    for i in range(self.__thread_count):
      t = multiprocessing.Process(target=OneInThreadProcessWrapper, kwargs={
        "i": i, "in_queue": in_queue, "process_fn": process_fn, "init_obj_fn": init_obj_fn})
      t.start()
      self.__processes.append(t)

  def JoinProcesses(self):
    if not self.__active:
      raise ValueError("try to join an inactive OneInProcessPool")
    self.__active = False
    for i in range(self.__thread_count):
      self.__in_queue.put(None)
    
    for i in range(self.__thread_count):
      self.__processes[i].join()

  def Join(self):
    self.JoinProcesses()

class OneInOneOutProcessPool:
  def __init__(self) -> None:
    pass

import time
def f(item, **kwargs):
  print(item)
  time.sleep(1)

if __name__ == "__main__":
  q = multiprocessing.Queue(5)
  pool = OneInProcessPool(4, q, f)
  for k in range(20):
    q.put(k)
  
  pool.JoinProcesses()