
def GetListValidIntervals(l, is_valid_fn):
  result = []
  for i in range(len(l)):
    if not is_valid_fn(l[i]):
      continue
    if len(result) == 0:
      result.append([i, i])
    else:
      last_interval = result[-1]
      if i - last_interval[1] == 1:
        # extend
        last_interval[1] = i
      else:
        # add new
        result.append([i, i])
  return result

def GetListSamePropertyIntervals(l, two_item_same_prop_fn):
  if len(l) == 0:
    return []
  result = [[0, 0]]
  for i in range(1, len(l)):
    last_interval = result[-1]
    if two_item_same_prop_fn(l[i-1], l[i]):
      # extend
      last_interval[1] = i
    else:
      # add new
      result.append([i, i])
  return result
