import os, sys
import re
import subprocess
import shutil
import logging
import chardet
from typing import List, Dict, Optional, Tuple, Pattern, Callable, Any

_decompressor_commandline_encoding = 'utf-8' # if sys.platform.startswith("win") else 'utf-8'

def run_command_safely(command):
    process = subprocess.run(
        command,
        capture_output=True,  # 捕获二进制输出
        text=False,           # 确保返回的是bytes而不是str
        check=False,
    )
    
    # 使用chardet检测编码并解码stdout
    if process.stdout:
        encoding_info = chardet.detect(process.stdout)
        stdout_decoded = process.stdout.decode(encoding_info['encoding'], errors='replace')
    else:
        stdout_decoded = ""
    
    # 使用chardet检测编码并解码stderr
    if process.stderr:
        encoding_info = chardet.detect(process.stderr)
        stderr_decoded = process.stderr.decode(encoding_info['encoding'], errors='replace')
    else:
        stderr_decoded = ""
    
    return process.returncode, stdout_decoded, stderr_decoded

class CommandLineDecompressor:
    """
    Generic base class for command-line decompression tools.
    Encapsulates common logic and provides global logging control.
    """
    
    # Class-level logging configuration
    _logger = logging.getLogger("CommandLineDecompressor")
    _log_enabled = True
    
    # Configure default logging handler
    if not _logger.handlers:
        _logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        _logger.addHandler(handler)
    
    @classmethod
    def enable_logging(cls, enable: bool):
        """
        Class method: Globally enable or disable logging
        
        :param enable: True to enable logging, False to disable
        """
        cls._log_enabled = enable
        if enable:
            cls._logger.setLevel(logging.INFO)
            cls._logger.info("Decompressor logging enabled")
        else:
            # Log disabling message before disabling
            cls._logger.info("Decompressor logging disabled")
            cls._logger.setLevel(logging.CRITICAL + 1)
    
    def __init__(self, 
                 executable_names: List[str], 
                 verification_keywords: List[str]):
        """
        Initialize the command-line decompressor
        
        :param executable_names: List of possible executable names
        :param verification_keywords: Keywords to look for in output during verification
        """
        self._executable_path: Optional[str] = None
        self._is_available: bool = False
        
        # Attempt to find and set the executable
        self._find_and_set_executable(executable_names, verification_keywords)

    def _log(self, level: int, message: str, *args, **kwargs):
        """Log a message respecting the global logging switch"""
        if self.__class__._log_enabled:
            self.__class__._logger.log(level, message, *args, **kwargs)

    def _find_and_set_executable(self, 
                                executable_names: List[str], 
                                verification_keywords: List[str]) -> None:
        """Attempt to find and set the executable in system PATH"""
        for name in executable_names:
            path = shutil.which(name)
            if path:
                self._log(logging.INFO, f"Attempting to verify '{name}' path: {path}")
                if self.set_executable_path(path, verification_keywords):
                    self._log(logging.INFO, f"Successfully verified '{name}'")
                    return
        
        self._log(logging.WARNING, "No valid executable found")
        self._is_available = False

    def set_executable_path(self, 
                            executable_path: str, 
                            verification_keywords: List[str]) -> bool:
        """
        Set and verify the executable path
        
        :param verification_keywords: Keywords to look for during verification
        :return: True if verification succeeds, False otherwise
        """
        if not os.path.exists(executable_path):
            self._log(logging.ERROR, f"Executable not found: {executable_path}")
            return False
            
        if self._verify_executable(executable_path, verification_keywords):
            self._executable_path = executable_path
            self._is_available = True
            return True
        return False

    def _verify_executable(self, 
                          executable_path: str, 
                          keywords: List[str]) -> bool:
        """Verify if the executable is valid"""
        try:
            process = subprocess.run(
                [executable_path], 
                capture_output=True, 
                text=True, 
                check=False, 
                timeout=10,
                encoding=_decompressor_commandline_encoding
            )
            output = process.stdout.lower() + process.stderr.lower()
            
            if any(keyword.lower() in output for keyword in keywords):
                self._log(logging.DEBUG, f"Verification succeeded: {executable_path}")
                return True
                
            self._log(logging.WARNING, f"Verification failed: {executable_path}")
            return False
        except Exception as e:
            self._log(logging.DEBUG, f"Verification error: {e}")
            return False

    def is_available(self) -> bool:
        """Check if the decompressor is available"""
        return self._is_available

    def _check_availability(self):
        """Check availability and raise exception if not available"""
        if not self._is_available or not self._executable_path:
            self._log(logging.CRITICAL, "Decompressor not initialized or unavailable")
            raise RuntimeError("Decompressor not initialized or unavailable")

    def _execute_command(self, command: List[str]) -> Tuple[int, str, str]:
        """Execute a command and return results"""
        self._check_availability()
        try:
            self._log(logging.DEBUG, f"Executing command: {' '.join(command)}")
            return run_command_safely(command)
            # process = subprocess.run(
            #     command, 
            #     capture_output=True, 
            #     text=True, 
            #     check=False,
            #     encoding=_decompressor_commandline_encoding
            # )
            # return process.returncode, process.stdout, process.stderr
        except Exception as e:
            self._log(logging.ERROR, f"Command execution error: {e}")
            return -1, "", str(e)

    def _parse_list_output(self, 
                          output: str, 
                          parse_rules: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Generic method to parse list command output with flexible rules
        
        :param output: Command output to parse
        :param parse_rules: Dictionary containing parsing configuration:
            - 'header_pattern': Regex to detect header line
            - 'header_separator_pattern': Regex to detect header separator
            - 'row_pattern': Regex to parse content rows
            - 'footer_pattern': Regex to detect footer/summary
            - 'skip_lines': List of patterns to skip
            - 'is_dir_indicator': String indicating directory entries
            - 'size_unit': Unit to append to size values
        :return: Parsed content list
        """
        contents = []
        in_content = False
        found_header = False
        found_separator = False
        
        # Extract rules with defaults
        header_pattern = parse_rules.get('header_pattern', re.compile(r'.*'))
        header_separator_pattern = parse_rules.get('header_separator_pattern', re.compile(r'^-+.*'))
        row_pattern = parse_rules.get('row_pattern')
        footer_pattern = parse_rules.get('footer_pattern', re.compile(r'.*'))
        skip_patterns = parse_rules.get('skip_patterns', [])
        is_dir_indicator = parse_rules.get('is_dir_indicator', 'D')
        size_unit = parse_rules.get('size_unit', ' Bytes')
        
        if not row_pattern:
            self._log(logging.ERROR, "No row_pattern provided for parsing")
            return contents
        
        for line in output.splitlines():
            stripped = line.strip()
            
            # Skip empty lines
            if not stripped:
                continue
                
            # Skip lines matching skip patterns
            if any(re.search(pattern, stripped) for pattern in skip_patterns):
                continue
                
            # Detect header line
            if not found_header and header_pattern.search(stripped):
                found_header = True
                continue
                
            # Detect header separator
            if found_header and not found_separator and header_separator_pattern.search(stripped):
                found_separator = True
                in_content = True
                continue
                
            # Detect footer/summary line
            if in_content and footer_pattern.search(stripped):
                break
                
            # Process content line
            if in_content and stripped:
                match = row_pattern.match(line)
                if match:
                    item_data = match.groupdict()
                    
                    # Handle directory detection
                    is_dir = False
                    if 'attributes' in item_data:
                        is_dir = is_dir_indicator in item_data['attributes']
                    elif 'type' in item_data:
                        is_dir = item_data['type'] == 'Dir'
                        
                    # Format size with unit
                    size = item_data.get('size', '0')
                    if size.isdigit():
                        size += size_unit
                    elif size == 'Directory':
                        is_dir = True
                    
                    # Create content item
                    content_item = {
                        'name': item_data.get('name', '').strip(),
                        'size': size if not is_dir else 'Directory',
                    }
                    
                    # Add optional fields
                    if 'date' in item_data and 'time' in item_data:
                        content_item['date_modified'] = f"{item_data['date']} {item_data['time']}"
                    elif 'date_modified' in item_data:
                        content_item['date_modified'] = item_data['date_modified']
                        
                    if 'attributes' in item_data:
                        content_item['attributes'] = item_data['attributes']
                        
                    if 'compressed' in item_data:
                        content_item['compressed_size'] = item_data['compressed'] + size_unit
                        
                    contents.append(content_item)
                else:
                    self._log(logging.DEBUG, f"Unable to parse line: {line}")
        
        return contents

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None, extra_switch: Optional[list[str]] = None) -> bool:
        """
        Decompress an archive file
        
        :param archive_path: Path to archive file
        :param output_path: Output directory
        :param password: Optional password
        :return: True on success, False on failure
        """
        raise NotImplementedError("Subclasses must implement this method")

    def list_contents(self, archive_path: str, password: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        List contents of an archive
        
        :param archive_path: Path to archive file
        :param password: Optional password
        :return: List of contents or None on failure
        """
        raise NotImplementedError("Subclasses must implement this method")

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Test archive integrity
        
        :param archive_path: Path to archive file
        :param password: Optional password
        :return: True on success, False on failure
        """
        raise NotImplementedError("Subclasses must implement this method")

    def get_supported_formats(self) -> List[str]:
        """
        Get list of supported formats
        
        :return: List of supported formats
        """
        raise NotImplementedError("Subclasses must implement this method")