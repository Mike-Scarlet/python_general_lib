from .interval import Interval
from typing import List, Optional, Union

class IntervalManager:
  def __init__(self, intervals: Optional[List[Interval]] = None):
    self.intervals: List[Interval] = intervals if intervals else []

  def _SortIntervals(self) -> None:
    self.intervals.sort(key=lambda x: x.GetStart())

  def AddInterval(self, interval: Interval) -> None:
    if not interval.IsValid():
      return

    intersecting_intervals = []
    for i, existing_interval in enumerate(self.intervals):
      if interval.Intersects(existing_interval):
        intersecting_intervals.append(i)

    if not intersecting_intervals:
      self.intervals.append(interval)
    else:
      start = min(interval.GetStart(), self.intervals[intersecting_intervals[0]].GetStart())
      end = max(interval.GetEnd(), self.intervals[intersecting_intervals[-1]].GetEnd())
      self.intervals[intersecting_intervals[0]:intersecting_intervals[-1]+1] = [Interval(start, end)]

  def RemoveInterval(self, interval: Interval) -> None:
    remove_intervals = [interval]

    for remove_interval in remove_intervals:
      new_intervals = []
      for existing_interval in self.intervals:
        if not remove_interval.Intersects(existing_interval):
          new_intervals.append(existing_interval)
        else:
          if remove_interval.GetStart() >= existing_interval.GetStart():
            new_intervals.append(Interval(existing_interval.GetStart(), remove_interval.GetStart()))
          if remove_interval.GetEnd() <= existing_interval.GetEnd():
            new_intervals.append(Interval(remove_interval.GetEnd(), existing_interval.GetEnd()))
      self.intervals = new_intervals

    self._SortIntervals()

  def GetIntervals(self) -> List[Interval]:
    return self.intervals

  def __str__(self) -> str:
    intervals_str = ', '.join(str(i) for i in self.intervals)
    return f'IntervalManager: {intervals_str}'