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

    self._SortIntervals()

  def RemoveInterval(self, interval: Interval) -> None:
    remove_intervals = [interval]

    for remove_interval in remove_intervals:
      new_intervals = []
      for existing_interval in self.intervals:
        if not remove_interval.Intersects(existing_interval):
          new_intervals.append(existing_interval)
        else:
          if remove_interval.GetStart() > existing_interval.GetStart():
            new_intervals.append(Interval(existing_interval.GetStart(), remove_interval.GetStart()))
          if remove_interval.GetEnd() < existing_interval.GetEnd():
            new_intervals.append(Interval(remove_interval.GetEnd(), existing_interval.GetEnd()))
      self.intervals = new_intervals

    self._SortIntervals()

  def GetIntervals(self) -> List[Interval]:
    return self.intervals
  
  def Intersection(self, other: 'IntervalManager') -> 'IntervalManager':
    intersection_manager = IntervalManager()
    
    i = 0
    j = 0
    while i < len(self.intervals) and j < len(other.intervals):
      interval1 = self.intervals[i]
      interval2 = other.intervals[j]
      
      if interval1.GetEnd() < interval2.GetStart():
        i += 1
      elif interval2.GetEnd() < interval1.GetStart():
        j += 1
      else:
        intersection = interval1.Intersection(interval2)
        if intersection.IsValid():
          intersection_manager.AddInterval(intersection)
        
        if interval1.GetEnd() < interval2.GetEnd():
          i += 1
        else:
          j += 1
    
    return intersection_manager
  
  def Union(self, other: 'IntervalManager') -> 'IntervalManager':
    union_manager = IntervalManager()
    
    for interval in self.intervals:
      union_manager.AddInterval(interval)
    
    for interval in other.intervals:
      union_manager.AddInterval(interval)
    
    return union_manager
  
  def Difference(self, other: 'IntervalManager') -> 'IntervalManager':
    difference_manager = IntervalManager()

    for interval in self.intervals:
      difference_manager.AddInterval(interval)

    for interval in other.intervals:
      difference_manager.RemoveInterval(interval)

    return difference_manager


  def __str__(self) -> str:
    intervals_str = ', '.join(str(i) for i in self.intervals)
    return f'IntervalManager: {intervals_str}'