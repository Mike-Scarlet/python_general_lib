from typing import Optional

class Interval:
  def __init__(self, start: Optional[float] = None, end: Optional[float] = None):
    self._start: Optional[float] = start
    self._end: Optional[float] = end
    self._UpdateValid()

  def _UpdateValid(self) -> None:
    self.valid = (self._start is not None) and (self._end is not None) and (self._start <= self._end)

  def GetStart(self) -> Optional[float]:
    return self._start

  def SetStart(self, start: Optional[float]) -> None:
    self._start = start
    self._UpdateValid()

  def GetEnd(self) -> Optional[float]:
    return self._end

  def SetEnd(self, end: Optional[float]) -> None:
    self._end = end
    self._UpdateValid()

  def IsValid(self) -> bool:
    return self.valid

  def Intersects(self, other: 'Interval') -> bool:
    """
    Returns True if this Interval intersects with the other Interval, False otherwise.

    Two Intervals intersect if there exists at least one value that is contained in both of them.
    """
    if not (self.valid and other.valid):
      return False
    return (self._start <= other._end) and (self._end >= other._start)

  def Intersection(self, other: 'Interval') -> 'Interval':
    """
    Returns a new Interval that represents the intersection of this Interval and the other Interval.

    If the two Intervals do not intersect, returns None.
    """
    if not (self.valid and other.valid):
      return Interval()
    start = max(self._start, other._start)
    end = min(self._end, other._end)
    return Interval(start, end)

  def Union(self, other: 'Interval') -> 'Interval':
    """
    Returns a list containing the Union of this Interval and the other Interval.

    If the two Intervals do not intersect, returns a list containing both of them.
    """
    if not (self.valid or other.valid):
      return Interval()
    if not self.valid:
      return other
    if not other.valid:
      return self
    start = min(self._start, other._start)
    end = max(self._end, other._end)
    return Interval(start, end)

  def __repr__(self):
    if self.IsValid():
      return "Interval({}, {})".format(self._start, self._end)
    else:
      return "Invalid Interval"

  def __str__(self) -> str:
    if self.valid:
      return 'Interval [{0}, {1}]'.format(self._start, self._end)
    else:
      return 'Invalid Interval'
    
  def __contains__(self, value):
    return self.IsValid() and (self._start <= value <= self._end)

  def __eq__(self, other):
    return self.IsValid() and other.IsValid() and (self._start == other._start) and (self._end == other._end)
