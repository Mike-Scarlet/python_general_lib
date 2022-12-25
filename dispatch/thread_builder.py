
__all__ = [
  "ThreadQueue",
  "OneInThreadPool",
  "OneInOneOutThreadPool",
]

import threading
import queue

ThreadQueue = queue.Queue

class OneInThreadPool:
  def __init__(self, thread_count, in_queue: queue.Queue, process_fn, init_obj_fn=None) -> None:
    def ThreadProcessWrapper(i: int, in_queue: queue.Queue):
      obj = None
      if init_obj_fn is not None:
        obj = init_obj_fn(i=i)
      while True:
        item = in_queue.get()
        if item is None:
          break
        process_fn(item=item, i=i, obj=obj)
    
    self.__in_queue = in_queue
    self.__threads = []
    self.__thread_count = thread_count
    self.__active = True
    for i in range(self.__thread_count):
      t = threading.Thread(target=ThreadProcessWrapper, kwargs={"i": i, "in_queue": in_queue})
      t.start()
      self.__threads.append(t)

  def JoinThreads(self):
    if not self.__active:
      raise ValueError("try to join an inactive OneInThreadPool")
    self.__active = False
    for i in range(self.__thread_count):
      self.__in_queue.put(None)
    
    for i in range(self.__thread_count):
      self.__threads[i].join()

  def Join(self):
    self.JoinThreads()

class OneInOneOutThreadPool:
  def __init__(self, thread_count, in_queue: queue.Queue, out_queue: queue.Queue, process_fn, init_obj_fn=None) -> None:
    def ThreadProcessWrapper(i: int, in_queue: queue.Queue, out_queue: queue.Queue):
      obj = None
      if init_obj_fn is not None:
        obj = init_obj_fn(i=i)
      while True:
        item = in_queue.get()
        if item is None:
          break
        result = process_fn(item=item, i=i, obj=obj)
        out_queue.put(result)

    self.__in_queue = in_queue
    self.__threads = []
    self.__thread_count = thread_count
    self.__active = True
    for i in range(self.__thread_count):
      t = threading.Thread(target=ThreadProcessWrapper, kwargs={"i": i, "in_queue": in_queue, "out_queue": out_queue})
      t.start()
      self.__threads.append(t)

  def JoinThreads(self):
    if not self.__active:
      raise ValueError("try to join an inactive OneInOneOutThreadPool")
    self.__active = False
    for i in range(self.__thread_count):
      self.__in_queue.put(None)
    
    for i in range(self.__thread_count):
      self.__threads[i].join()