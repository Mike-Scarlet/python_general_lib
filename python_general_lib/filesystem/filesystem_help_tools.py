
import os

def CreateNeededFolderGivenPath(path):
  parent_folder = os.path.dirname(path)
  if not os.path.exists(parent_folder):
    os.makedirs(parent_folder, exist_ok=True)