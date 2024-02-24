
def SplitListBySize(l, size):
  for i in range(0, len(l), size):
    yield l[i: i+size]

if __name__ == "__main__":
  lst = [1, 2, 34, 5, 6, 7, 89, 9, 0]
  for _ in SplitListBySize(lst, 9):
    print(_)