
import time

class TimeInstance:
  def __init__(self) -> None:
    self.create_time = time.time()

  def ElapsedTime(self):
    return time.time() - self.create_time