from python_general_lib.data_structure.interval_manager import *

def test_interval_manager():
  # Create an empty IntervalManager
  manager = IntervalManager()

  # Add some intervals to the manager
  manager.AddInterval(Interval(0, 5))
  manager.AddInterval(Interval(10, 15))
  manager.AddInterval(Interval(20, 25))

  # Check that the intervals were added correctly
  assert manager.GetIntervals() == [Interval(0, 5), Interval(10, 15), Interval(20, 25)]

  # Try adding an invalid interval
  manager.AddInterval(Interval(30, 20))
  assert manager.GetIntervals() == [Interval(0, 5), Interval(10, 15), Interval(20, 25)]

  # Add an interval that intersects with two existing intervals
  manager.AddInterval(Interval(3, 12))
  assert manager.GetIntervals() == [Interval(0, 15), Interval(20, 25)]

  # Remove an interval that lies entirely within an existing interval
  manager.RemoveInterval(Interval(2, 4))
  assert manager.GetIntervals() == [Interval(0, 2), Interval(4, 15), Interval(20, 25)]

  # Remove an interval that splits an existing interval in two
  manager.RemoveInterval(Interval(4, 12))
  assert manager.GetIntervals() == [Interval(0, 2), Interval(12, 15), Interval(20, 25)]

  # Remove an interval that completely contains an existing interval
  manager.RemoveInterval(Interval(5, 20))
  assert manager.GetIntervals() == [Interval(0, 2), Interval(20, 25)]

  manager.AddInterval(Interval(4, 4))
  assert manager.GetIntervals() == [Interval(0, 2), Interval(4, 4), Interval(20, 25)]

  manager.AddInterval(Interval(4, 5))
  assert manager.GetIntervals() == [Interval(0, 2), Interval(4, 5), Interval(20, 25)]

def test_interval_manager_intersection():
  manager1 = IntervalManager([Interval(1, 5), Interval(6, 10), Interval(12, 15)])
  manager2 = IntervalManager([Interval(2, 8), Interval(9, 10), Interval(14, 16)])
  manager3 = IntervalManager([Interval(2, 8), Interval(10, 11), Interval(14, 16)])
  
  result = manager1.Intersection(manager2)
  print(result)

  result2 = manager1.Union(manager3)
  print(result2)

  result3 = manager1.Difference(manager3)
  print(result3)

if __name__ == "__main__":
  # test_interval_manager()
  test_interval_manager_intersection()