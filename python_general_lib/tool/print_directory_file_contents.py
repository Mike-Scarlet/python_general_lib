
import os

def PrintDirectoryFileContents(root_folder):
  blacklist = [
    "gitignore",
    ".pyc",
  ]
  for root, dirs, files in os.walk(root_folder):
    for file in files:
      file_path = os.path.join(root, file)
      
      skip = False
      for black_str in blacklist:
        if black_str in file_path:
          skip = True
          break
      if skip:
        continue

      print(f"{file_path.replace(root_folder, '')}文件内容:")
      
      with open(file_path, 'r', encoding='utf-8') as f:
        print("```")
        content = f.read()
        print(content.strip())
        print("```\n")

def SaveDirectoryFileContents(root_folder, save_path):
  blacklist = [
    "gitignore",
    ".pyc",
    "__init__.py",
  ]
  lines = []
  for root, dirs, files in os.walk(root_folder):
    for file in files:
      file_path = os.path.join(root, file)
      
      skip = False
      for black_str in blacklist:
        if black_str in file_path:
          skip = True
          break
      if skip:
        continue

      lines.append(f"{file_path.replace(root_folder, '')}文件内容:")
      
      with open(file_path, 'r', encoding='utf-8') as f:
        lines.append("```")
        content = f.read()
        lines.append(content.strip())
        lines.append("```\n")
  with open(save_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines))


if __name__ == "__main__":
  # 示例：调用函数遍历指定目录
  root_folder = '/media/ubuntu/data/DevelopEnvironment/tools/misc/python_general_lib/python_general_lib/database/sqlite3_wrap'  # 替换为实际的目录路径
  # PrintDirectoryFileContents(root_folder)
  SaveDirectoryFileContents(root_folder, "prompt.txt")