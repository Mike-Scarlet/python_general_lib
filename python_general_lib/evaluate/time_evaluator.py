import time
import threading
from contextlib import ContextDecorator

class TimeEvaluator:
  _instance = None
  _lock = threading.Lock()

  def __new__(cls):
    if not cls._instance:
      with cls._lock:
        if not cls._instance:
          cls._instance = super(TimeEvaluator, cls).__new__(cls)
          cls._instance._stats = {}
          cls._instance._stats_lock = threading.Lock()
    return cls._instance

  def _update_stats(self, name, elapsed):
    with self._stats_lock:
      if name not in self._stats:
        self._stats[name] = {
          'count': 0,
          'total_time': 0.0,
          'max_time': 0.0,
          'min_time': float('inf')
        }
      stat = self._stats[name]
      stat['count'] += 1
      stat['total_time'] += elapsed
      stat['max_time'] = max(stat['max_time'], elapsed)
      stat['min_time'] = min(stat['min_time'], elapsed)

  def GetStats(self):
    with self._stats_lock:
      # Return a copy to prevent external modification
      return {k: v.copy() for k, v in self._stats.items()}

  def PrintStats(self):
    stats = self.GetStats()
    if not stats:
      print("No timing statistics available.")
      return
    header = "{:<20} {:<10} {:<15} {:<15} {:<15} {:<15}".format(
      "Name", "Count", "Total Time (s)", "Avg Time (s)", "Min Time (s)", "Max Time (s)"
    )
    print(header)
    print("-" * 90)
    for name, data in stats.items():
      avg_time = data['total_time'] / data['count'] if data['count'] else 0
      line = "{:<20} {:<10} {:<15.6f} {:<15.6f} {:<15.6f} {:<15.6f}".format(
        name, data['count'], data['total_time'], avg_time, data['min_time'], data['max_time']
      )
      print(line)

class TimerContext:
  def __init__(self, name=None):
    self.name = name if name else "default"
    self.evaluator = TimeEvaluator()

  def __enter__(self):
    self.start_time = time.time()

  def __exit__(self, exc_type, exc_val, exc_tb):
    end_time = time.time()
    elapsed = end_time - self.start_time
    self.evaluator._update_stats(self.name, elapsed)

class TimerDecorator(ContextDecorator):
  def __init__(self, func=None, name=None):
    self.func = func
    self.name = name
    self.evaluator = TimeEvaluator()
    if func:
      self.__name__ = func.__name__

  def __call__(self, *args, **kwargs):
    name = self.name if self.name else self.func.__name__
    start_time = time.time()
    try:
      return self.func(*args, **kwargs)
    finally:
      end_time = time.time()
      elapsed = end_time - start_time
      self.evaluator._update_stats(name, elapsed)

  def __get__(self, obj, objtype):
    # Support instance methods
    return self.__class__(self.func.__get__(obj, objtype), self.name)

def TimeitContext(name=None):
  return TimerContext(name)

def TimeitDecorator(name=None):
  def decorator(func):
    return TimerDecorator(func, name)
  return decorator

# Expose a unified decorator that can be used with or without arguments
def Timeit(*args, **kwargs):
  if len(args) == 1 and callable(args[0]):
    # Used as @Timeit without arguments
    return TimerDecorator(args[0])
  else:
    # Used as @Timeit(name="custom_name")
    return TimeitDecorator(*args, **kwargs)
	
# Print formatted statistics
def TimeEvaluatorPrintStats():
  evaluator = TimeEvaluator()
  evaluator.PrintStats()


"""prompt >> GPT o1-mini

请帮我写一个进行python代码段时间评估的库，要求有的功能如下
1. 有一个能够评估一段代码运行时间的上下文管理器
2. 有一个能够评估一个函数运行时间的装饰器
3. 上下文管理器和装饰器，可以被命名，如果有命名的话每次运行完代码段之后，会把名字相同的段的运行时间统计出来，提供一个方法获取统计结果，提供一个方法基于统计结果打印具体信息
4. 所有处理是线程安全的
"""