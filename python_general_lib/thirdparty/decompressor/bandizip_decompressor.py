
import os
import re
import subprocess
import shutil
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.command_line_decompressor import CommandLineDecompressor

class BandizipDecompressor(CommandLineDecompressor):
    def __init__(self):
        # Bandizip的可执行文件名称列表和验证关键词
        super().__init__(
            executable_names=["bz.exe", "bz"],
            verification_keywords=["Bandizip console tool"]
        )
        
    def add_password_to_command(self, command, password=None):
        if password:
            command.append(f"-p:{password}")
        else:
            command.append(f'-p:""')
    
    def get_supported_formats(self) -> List[str]:
        """返回Bandizip支持的压缩格式列表"""
        return ["zip", "zipx", "exe", "tar", "tgz", "lzh", "iso", "7z", "gz", "xz"]

    def list_contents(self, 
                     archive_path: str, 
                     password: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        列出压缩包内容
        命令格式: bz l <archive_path>
        """
        self._check_availability()
        if not os.path.exists(archive_path):
            self._log(logging.ERROR, f"Archive not found: {archive_path}")
            return None

        # 构建命令
        cmd = [self._executable_path, "l", "-y"]
        self.add_password_to_command(cmd, password)
        cmd.append(archive_path)
        
        # 执行命令
        returncode, stdout, stderr = self._execute_command(cmd)
        
        # 处理命令执行结果
        if returncode != 0:
            self._log(logging.ERROR, f"List failed ({returncode}): {stderr.strip()}")
            return None
        
        # 定义解析规则
        parse_rules = {
            "header_pattern": re.compile(r"^\s*Date\s+Time\s+Attr\s+Size\s+CompSize\s+Name", re.IGNORECASE),
            "header_separator_pattern": re.compile(r"^-+$"),
            "row_pattern": re.compile(
                r"^(?P<date>\d{4}-\d{2}-\d{2})\s+"           # 日期
                r"(?P<time>\d{2}:\d{2}:\d{2})\s+"           # 时间
                r"(?P<attributes>\S{0,4})\s+"               # 属性（可能缺失）
                r"(?P<size>\d+|Directory)\s+"               # 大小或"Directory"
                r"(?P<compressed>\d+|Directory)?\s*"        # 压缩大小（可能缺失）
                r"(?P<name>.+)$"                            # 文件名
            ),
            "footer_pattern": re.compile(r"^\d+\s+files?\s*,\s*\d+\s+folders?\s*$", re.IGNORECASE),
            "skip_patterns": [
                re.compile(r"^Archive format:.*$"),
                re.compile(r"^bz\s+.+$"),
                re.compile(r"^Listing archive:.*$"),
                re.compile(r"^-+[\s-]*$")
            ],
            "is_dir_indicator": "D",
            "size_unit": " Bytes"
        }
        
        return self._parse_list_output(stdout, parse_rules)

    def decompress(self, 
                   archive_path: str, 
                   output_path: str, 
                   password: Optional[str] = None,
                   extra_switch: Optional[List[str]] = None) -> bool:
        """
        解压文件
        命令格式: bz x -aoa -o:<output_path> -p:<password> <archive_path>
        """
        self._check_availability()
        if not os.path.exists(archive_path):
            self._log(logging.ERROR, f"Archive not found: {archive_path}")
            return False

        # 确保输出目录存在
        os.makedirs(output_path, exist_ok=True)
        
        # 构建基础命令
        cmd = [
            self._executable_path,
            "x",  # 完整路径解压
            "-aoa",  # 覆盖所有现有文件
            "-y",
            f"-o:{output_path}",
        ]
        # 添加密码参数
        self.add_password_to_command(cmd, password)
        cmd.append(archive_path)
                
        # 添加额外参数
        if extra_switch:
            cmd.extend(extra_switch)
        
        # 执行命令
        returncode, stdout, stderr = self._execute_command(cmd)
        
        if returncode == 0:
            self._log(logging.INFO, f"Decompression successful: {archive_path} -> {output_path}")
            return True
        else:
            self._log(logging.ERROR, f"Decompression failed ({returncode}): {stderr.strip()}")
            return False

    def test_archive(self, 
                    archive_path: str, 
                    password: Optional[str] = None) -> bool:
        """
        测试压缩包完整性
        命令格式: bz t -p:<password> <archive_path>
        """
        self._check_availability()
        if not os.path.exists(archive_path):
            self._log(logging.ERROR, f"Archive not found: {archive_path}")
            return False

        # 构建命令
        cmd = [self._executable_path, "t", "-y"]
        self.add_password_to_command(cmd, password)
        cmd.append(archive_path)
        
        # 执行命令
        returncode, stdout, stderr = self._execute_command(cmd)
        
        if returncode == 0:
            self._log(logging.INFO, f"Archive test passed: {archive_path}")
            return True
        else:
            self._log(logging.ERROR, f"Archive test failed ({returncode}): {stderr.strip()}")
            return False

if __name__ == "__main__":
    import zipfile
    import tempfile
    CommandLineDecompressor.enable_logging(True) # Enable logging for all decompressor instances

    print("--- Instantiating BandizipDecompressor ---")
    # 创建临时工作目录
    temp_dir = tempfile.mkdtemp()
    print(f"Created temporary directory: {temp_dir}")
    
    try:
        # 创建测试压缩包路径
        test_archive = os.path.join(temp_dir, "test_archive_bz.zip")
        print(f"Test archive path: {test_archive}")
        
        # 创建测试压缩包内容
        test_content_dir = os.path.join(temp_dir, "test_content")
        os.makedirs(test_content_dir, exist_ok=True)
        
        # 创建测试文件
        with open(os.path.join(test_content_dir, "file1.txt"), "w") as f:
            f.write("This is test file 1")
        
        with open(os.path.join(test_content_dir, "file2.txt"), "w") as f:
            f.write("This is test file 2")
        
        # 创建子目录
        sub_dir = os.path.join(test_content_dir, "subdir")
        os.makedirs(sub_dir, exist_ok=True)
        
        with open(os.path.join(sub_dir, "file3.txt"), "w") as f:
            f.write("This is test file 3 in subdirectory")
        
        # 创建 ZIP 压缩包
        print("Creating test ZIP archive...")
        with zipfile.ZipFile(test_archive, 'w') as zipf:
            for root, _, files in os.walk(test_content_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, test_content_dir)
                    zipf.write(file_path, arcname)
        
        print(f"Created test archive with {len(zipf.filelist)} files")
        
        # 创建 BandizipDecompressor 实例
        decompressor = BandizipDecompressor()
        
        if not decompressor.is_available():
            print("Bandizip not available, skipping tests")
            exit(1)
        
        print("Bandizip decompressor is available")
        
        # 测试列表功能
        print("\n=== Testing list_contents ===")
        contents = decompressor.list_contents(test_archive)
        
        if contents is None:
            print("List contents failed")
        else:
            print(f"Found {len(contents)} items in archive:")
            for item in contents:
                print(f"  - {item['name']} ({item['size']})")
            
            # 验证特定文件是否存在
            expected_files = ["file1.txt", "file2.txt", "subdir/file3.txt"]
            found_files = [item['name'] for item in contents]
            
            for file in expected_files:
                if file not in found_files:
                    print(f"ERROR: Expected file '{file}' not found in archive")
                else:
                    print(f"Verified file '{file}' exists in archive")
        
        # 测试解压缩功能
        print("\n=== Testing decompress ===")
        output_dir = os.path.join(temp_dir, "output")
        print(f"Output directory: {output_dir}")
        
        success = decompressor.decompress(test_archive, output_dir)
        
        if not success:
            print("Decompression failed")
        else:
            print("Decompression successful")
            
            # 验证解压后的文件
            expected_files = [
                ("file1.txt", "This is test file 1"),
                ("file2.txt", "This is test file 2"),
                ("subdir/file3.txt", "This is test file 3 in subdirectory")
            ]
            
            all_verified = True
            for file, expected_content in expected_files:
                file_path = os.path.join(output_dir, file)
                
                if not os.path.exists(file_path):
                    print(f"ERROR: File not found: {file_path}")
                    all_verified = False
                    continue
                
                with open(file_path, "r") as f:
                    content = f.read()
                    
                if content != expected_content:
                    print(f"ERROR: Content mismatch for {file}")
                    print(f"  Expected: {expected_content}")
                    print(f"  Actual: {content}")
                    all_verified = False
                else:
                    print(f"Verified content for {file}")
            
            if all_verified:
                print("All files verified successfully")
    
    finally:
        # 清理临时目录
        print("\nCleaning up temporary directory...")
        shutil.rmtree(temp_dir)
        print("Cleanup complete")
