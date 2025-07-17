
import os

def CreateFolderIfNotExists(path):
  if not os.path.exists(path):
    os.makedirs(path)

def CreateNeededFolderGivenPath(path):
  parent_folder = os.path.dirname(path)
  if not os.path.exists(parent_folder):
    os.makedirs(parent_folder, exist_ok=True)