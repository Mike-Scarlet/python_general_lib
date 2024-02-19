
import bisect

__all__ = [
  "KeyWrapper",
  "bisect_left",
  "bisect_right",
  "lower_bound",
  "upper_bound",
  "first_ge",
  "first_g",
  "last_le",
  "last_l",
]

class KeyWrapper:
  def __init__(self, iterable, key) -> None:
    self.it = iterable
    self.key = key

  def __getitem__(self, i):
    return self.key(self.it[i])

  def __len__(self):
    return len(self.it)
  
def bisect_left(a, x, lo=0, hi=None, key=None):
  if key is None:
    return bisect.bisect_left(a, x, lo, hi)
  else:
    wrap = KeyWrapper(a, key)
    return bisect.bisect_left(wrap, x, lo, hi)

def bisect_right(a, x, lo=0, hi=None, key=None):
  if key is None:
    return bisect.bisect_right(a, x, lo, hi)
  else:
    wrap = KeyWrapper(a, key)
    return bisect.bisect_right(wrap, x, lo, hi)
  
def lower_bound(a, x, lo=0, hi=None, key=None):
  return bisect_left(a, x, lo, hi, key)

def upper_bound(a, x, lo=0, hi=None, key=None):
  return bisect_right(a, x, lo, hi, key)

def first_ge(a, x, lo=0, hi=None, key=None):
  return bisect_left(a, x, lo, hi, key)

def first_g(a, x, lo=0, hi=None, key=None):
  return bisect_right(a, x, lo, hi, key)

def last_le(a, x, lo=0, hi=None, key=None):
  return bisect_right(a, x, lo, hi, key) - 1

def last_l(a, x, lo=0, hi=None, key=None):
  return bisect_left(a, x, lo, hi, key) - 1
  
if __name__ == "__main__":
  a = [1, 2, 3, 3, 4, 4, 5, 5, 7, 8]

  def my_bisect_left(a, x):
    left, right = 0, len(a) - 1
    # close
    while left <= right:
      mid = (left + right) // 2
      if a[mid] < x:
        left = mid + 1
      else:
        right = mid - 1
    return left

  def my_bisect_right(a, x):
    left, right = 0, len(a) - 1
    # close
    while left <= right:
      mid = (left + right) // 2
      if a[mid] > x:
        right = mid - 1
      else:
        left = mid + 1
    return left
  
  # print(my_bisect_left(a, 3))
  # print(my_bisect_right(a, 3))
  # print(my_bisect_left(a, 4))
  # print(my_bisect_right(a, 4))
  # print(my_bisect_left(a, 5))
  # print(my_bisect_right(a, 5))
  print(first_ge(a, 4))
  print(first_g(a, 4))
  print(last_le(a, 4))
  print(last_l(a, 4))
  pass