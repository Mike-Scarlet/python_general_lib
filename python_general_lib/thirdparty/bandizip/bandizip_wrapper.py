
import subprocess
import re

_bandizip_console_path = ""

def SetBandizipConsolePath(path):
  global _bandizip_console_path
  _bandizip_console_path = path
  
def ListZipFile(file_path, password=None):
  options = ""
  if not password:
    args = [_bandizip_console_path, "l", file_path]
  else:
    args = [_bandizip_console_path, "l", f"-p:{password}", file_path]
  exe = subprocess.Popen(args=args, 
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, stderr = exe.communicate()
  stdout_str = stdout.decode()
  
  # do parse
  results = []
  pattern = r"([0-9\-]+) ([0-9:]+) (\w+) +(\d+) +(\d+) +(.*)"
  for line in stdout_str.split("\n"):
    line_strip = line.strip()
    match_result = re.match(pattern, line_strip)
    if match_result:
      results.append(match_result.groups())
  return results
  
if __name__ == "__main__":
  pass