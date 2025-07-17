import os
import re
import subprocess
import shutil # For shutil.which() to find executables in PATH
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.command_line_decompressor import CommandLineDecompressor

# Assuming Decompressor base class is defined as in the previous turns

class UnRARDecompressor(CommandLineDecompressor):
    """Implementation for RAR decompression using unrar"""
    
    def __init__(self):
        super().__init__(
            executable_names=["unrar", "unrar.exe"],
            verification_keywords=["unrar", "rar archive", "copyright", "usage"]
        )

    def add_password_to_command(self, command, password=None):
        if password:
            command.append(f'-p{password}')
        else:
            command.append(f'-p""')

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None, extra_switch: Optional[list[str]] = None) -> bool:
        """
        Decompress a RAR archive
        
        :param archive_path: Path to RAR archive
        :param output_path: Output directory
        :param password: Optional password
        :return: True on success, False on failure
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        # Build decompression command
        command = [self._executable_path, 'x', archive_path, output_path, '-o+']
        self.add_password_to_command(command, password)
            
        # Execute command and check result
        returncode, stdout, stderr = self._execute_command(command)
        
        # Log results
        if returncode == 0:
            self._log(logging.INFO, f"Successfully decompressed '{archive_path}' to '{output_path}'")
            return True
        else:
            self._log(logging.ERROR, f"Decompression failed for '{archive_path}', error code: {returncode}")
            self._log(logging.DEBUG, f"STDOUT: {stdout}")
            self._log(logging.DEBUG, f"STDERR: {stderr}")
            return False

    def list_contents(self, archive_path: str, password: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        List contents of a RAR archive
        
        :param archive_path: Path to RAR archive
        :param password: Optional password
        :return: List of contents or None on failure
        """
        # Build list command
        command = [self._executable_path, 'l', archive_path]
        self.add_password_to_command(command, password)
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Define parsing rules for unrar output
        parse_rules = {
            'header_pattern': re.compile(r'Attributes\s+Size\s+Date\s+Time\s+Name'),
            'header_separator_pattern': re.compile(r'^-+\s+-+\s+-+\s+-+\s+-+$'),
            'row_pattern': re.compile(
                r'^\s*(?P<attributes>[a-zA-Z-]+)\s+'
                r'(?P<size>\d+)\s+'
                r'(?P<date>\d{4}-\d{2}-\d{2})\s+'
                r'(?P<time>\d{2}:\d{2})\s+'
                r'(?P<name>.*)$'
            ),
            'footer_pattern': re.compile(r'^-+\s+-+\s+-+\s+-+\s+-+$'),
            'skip_patterns': [
                r'^UNRAR\s+\d',  # Skip version line
                r'^Archive:',     # Skip archive info
                r'^Details:',      # Skip details
            ],
            'is_dir_indicator': 'd',  # Lowercase 'd' in attributes indicates directory
        }
        
        contents = self._parse_list_output(stdout, parse_rules)
        self._log(logging.INFO, f"Successfully listed '{archive_path}', found {len(contents)} items")
        return contents

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Test integrity of a RAR archive
        
        :param archive_path: Path to RAR archive
        :param password: Optional password
        :return: True on success, False on failure
        """
        # Build test command
        command = [self._executable_path, 't', archive_path]
        self.add_password_to_command(command, password)
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Check result
        if returncode == 0:
            self._log(logging.INFO, f"Archive '{archive_path}' integrity test successful")
            return True
        else:
            self._log(logging.ERROR, f"Archive '{archive_path}' integrity test failed, error code: {returncode}")
            self._log(logging.DEBUG, f"STDOUT: {stdout}")
            self._log(logging.DEBUG, f"STDERR: {stderr}")
            return False

    def get_supported_formats(self) -> List[str]:
        """
        Get list of supported formats
        
        :return: List of supported formats (['rar'])
        """
        self._log(logging.INFO, "Querying supported formats - RAR")
        return ['rar']

# --- Example Usage ---
if __name__ == "__main__":
    # Create an instance of UnRARDecompressor
    print("--- Instantiating UnRARDecompressor ---")
    unrar_decompressor = UnRARDecompressor()
    
    # Check initial availability
    print(f"Initial availability: {unrar_decompressor.is_available()}") 

    # --- Test Case 1: Attempt operation if not available ---
    print("\n--- Testing operation if not initially available ---")
    if not unrar_decompressor.is_available():
        try:
            unrar_decompressor.decompress("dummy.rar", "output_dir")
        except RuntimeError as e:
            print(f"Caught expected error: {e}")
    else:
        print("Decompressor is initially available, skipping unavailable operation test.")

    # --- Test Case 2: Manually set an invalid path if not available ---
    if not unrar_decompressor.is_available():
        print("\n--- Decompressor not initially available. Trying to set an invalid path ---")
        invalid_path = "/path/to/definitely/not/unrar"
        if not unrar_decompressor.set_executable_path(invalid_path):
            print(f"Correctly failed to set path: {invalid_path}")
        print(f"Availability after invalid path: {unrar_decompressor.is_available()}")

        # --- Test Case 3: Manually set a valid path if not available ---
        print("\n--- Decompressor not initially available. Trying to set a valid custom path ---")
        # IMPORTANT: Replace with the actual path to your 'unrar' executable
        # if it's not found in PATH or you want to use a specific version.
        # e.g., custom_unrar_path = r"C:\path\to\your\UnRAR.exe" or "/opt/unrar/bin/unrar"
        custom_unrar_path = shutil.which("unrar") # Try to get it from PATH if we missed it earlier
        if not custom_unrar_path: # Fallback for manual testing if shutil.which fails
            # Add common manual paths here if you want to hardcode for testing
            if os.name == 'nt':
                 custom_unrar_path = os.path.join(os.environ.get('ProgramFiles', 'C:\\Program Files'), 'WinRAR', 'UnRAR.exe')
            else:
                 custom_unrar_path = '/usr/bin/unrar' # Common Linux/macOS path
                 if not os.path.exists(custom_unrar_path):
                     custom_unrar_path = '/usr/local/bin/unrar'

        if custom_unrar_path and os.path.exists(custom_unrar_path):
            print(f"Attempting to set custom path: {custom_unrar_path}")
            if unrar_decompressor.set_executable_path(custom_unrar_path):
                print(f"Manually set and verified 'unrar' successfully. Availability: {unrar_decompressor.is_available()}")
            else:
                print(f"Failed to manually set '{custom_unrar_path}'. Availability: {unrar_decompressor.is_available()}")
        else:
            print("No valid custom 'unrar' path found for manual testing. Please set one if needed.")

    # --- Perform operations if available (either from initial check or manual set) ---
    if unrar_decompressor.is_available():
        print("\n--- Decompressor is available. Attempting operations... ---")
        
        test_archive_name = "test_archive.rar" # Ensure you have this file for testing
        test_output_dir = "unrar_output"
        
        if not os.path.exists(test_archive_name):
            print(f"\nWarning: '{test_archive_name}' not found. Skipping decompression/listing/testing demo.")
            print("Please create a test.rar file (e.g., using WinRAR) to test these functionalities.")
        else:
            print(f"\n--- Testing decompression of '{test_archive_name}' ---")
            if unrar_decompressor.decompress(test_archive_name, test_output_dir):
                print(f"Decompression of '{test_archive_name}' successful.")
                # Optional cleanup
                # import shutil
                # if os.path.exists(test_output_dir):
                #     shutil.rmtree(test_output_dir)
            else:
                print(f"Decompression of '{test_archive_name}' failed.")

            print(f"\n--- Testing listing contents of '{test_archive_name}' ---")
            contents = unrar_decompressor.list_contents(test_archive_name)
            if contents:
                print(f"Contents of '{test_archive_name}':")
                for item in contents:
                    print(f"  Name: {item.get('name')}, Size: {item.get('size')}, Date: {item.get('date_modified')}")
            else:
                print(f"Failed to list contents of '{test_archive_name}'.")

            print(f"\n--- Testing integrity of '{test_archive_name}' ---")
            if unrar_decompressor.test_archive(test_archive_name):
                print(f"Integrity test of '{test_archive_name}' successful.")
            else:
                print(f"Integrity test of '{test_archive_name}' failed.")