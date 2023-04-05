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
  assert manager.GetIntervals() == [Interval(0, 2), Interval(4, 4), Interval(12, 15), Interval(20, 25)]

  # Remove an interval that completely contains an existing interval
  manager.RemoveInterval(Interval(5, 20))
  assert manager.GetIntervals() == [Interval(0, 2), Interval(4, 4), Interval(20, 25)]

  manager.AddInterval(Interval(4, 4))
  assert manager.GetIntervals() == [Interval(0, 2), Interval(4, 4), Interval(20, 25)]

if __name__ == "__main__":
  test_interval_manager()