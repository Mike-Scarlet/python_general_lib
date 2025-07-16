import os
import re
import subprocess
import shutil
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.command_line_decompressor import CommandLineDecompressor

# Assuming Decompressor base class is defined as in the previous turns

class UnZIPDecompressor(CommandLineDecompressor):
    """
    Implementation for ZIP decompression using unzip command-line utility.
    Supports listing, decompressing and testing ZIP archives.
    """
    
    def __init__(self):
        """
        Initialize the UnZIP decompressor.
        Attempts to find 'unzip' in system PATH and verifies it.
        """
        super().__init__(
            executable_names=["unzip"],
            verification_keywords=["unzip", "pkzip", "copyright", "usage"]
        )

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None) -> bool:
        """
        Decompress a ZIP archive
        
        :param archive_path: Path to ZIP archive
        :param output_path: Output directory
        :param password: Optional password
        :return: True on success, False on failure
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        # Build decompression command
        command = [
            self._executable_path, 
            '-o',  # Overwrite files without prompting
            archive_path, 
            '-d', output_path  # Specify output directory
        ]
        
        # Add password if provided
        if password:
            command.extend(['-P', password])
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Check result (unzip returns 0 for success, 1 for warnings)
        if returncode in (0, 1):
            self._log(logging.INFO, f"Successfully decompressed '{archive_path}' to '{output_path}'")
            return True
        else:
            self._log(logging.ERROR, f"Decompression failed for '{archive_path}', error code: {returncode}")
            self._log(logging.DEBUG, f"STDOUT: {stdout}")
            self._log(logging.DEBUG, f"STDERR: {stderr}")
            return False

    def list_contents(self, archive_path: str, password: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        List contents of a ZIP archive
        
        :param archive_path: Path to ZIP archive
        :param password: Optional password
        :return: List of contents or None on failure
        """
        # Build list command
        command = [self._executable_path, '-l', archive_path]
        if password:
            command.extend(['-P', password])
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Check command success (unzip returns 0 for success, 1 for warnings)
        if returncode not in (0, 1):
            self._log(logging.ERROR, f"Listing failed for '{archive_path}', error code: {returncode}")
            self._log(logging.DEBUG, f"STDOUT: {stdout}")
            self._log(logging.DEBUG, f"STDERR: {stderr}")
            return None
            
        # Define parsing rules for unzip output
        parse_rules = {
            'header_pattern': re.compile(r'^\s*Length\s+Date\s+Time\s+Name\s*$'),
            'header_separator_pattern': re.compile(r'^-+\s+-+\s+-+\s+-+$'),
            'row_pattern': re.compile(
                r'^\s*(?P<size>\d+)\s+'          # Size (digits)
                r'(?P<date>\d{4}-\d{2}-\d{2})\s+'  # Date (YYYY-MM-DD)
                r'(?P<time>\d{2}:\d{2})\s+'      # Time (HH:MM)
                r'(?P<name>.*)$'                 # Name (rest of line)
            ),
            'footer_pattern': re.compile(r'^-+\s+\d+\s+files?$'),
            'skip_patterns': [
                r'^Archive:\s+',  # Skip archive info line
                r'^\d+\s+files?', # Skip file count summary
            ],
            'is_dir_indicator': '/',  # Names ending with '/' are directories
            'size_unit': ' Bytes',
        }
        
        # Parse output content
        contents = self._parse_list_output(stdout, parse_rules)
        self._log(logging.INFO, f"Successfully listed '{archive_path}', found {len(contents)} items")
        return contents

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Test integrity of a ZIP archive
        
        :param archive_path: Path to ZIP archive
        :param password: Optional password
        :return: True on success, False on failure
        """
        # Build test command
        command = [self._executable_path, '-t', archive_path]
        if password:
            command.extend(['-P', password])
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Check result (unzip returns 0 for success, 1 for warnings)
        if returncode in (0, 1):
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
        
        :return: List of supported formats (['zip'])
        """
        self._log(logging.INFO, "Querying supported formats - ZIP")
        return ['zip']


# --- Example Usage ---
if __name__ == "__main__":
    # Ensure logging is enabled for visibility
    CommandLineDecompressor.enable_logging(True) 
    print("--- Testing UnZIPDecompressor ---")

    # Instantiate UnZIPDecompressor
    unzip_decompressor = UnZIPDecompressor()
    print(f"Initial availability: {unzip_decompressor.is_available()}")

    # --- Test operations only if unzip is available ---
    if unzip_decompressor.is_available():
        test_zip_archive = "test_archive.zip" # You need to create this file
        test_output_dir = "unzip_output"

        # Create a dummy test_archive.zip for demonstration
        # This requires Python's zipfile module (standard library)
        import zipfile
        if not os.path.exists(test_zip_archive):
            CommandLineDecompressor._logger.info(f"Creating dummy '{test_zip_archive}' for testing...")
            os.makedirs("temp_files_for_zip_test", exist_ok=True)
            with open("temp_files_for_zip_test/file1.txt", "w") as f:
                f.write("This is file 1.")
            with open("temp_files_for_zip_test/file2.txt", "w") as f:
                f.write("This is file 2.")
            
            with zipfile.ZipFile(test_zip_archive, 'w') as zf:
                zf.write("temp_files_for_zip_test/file1.txt", arcname="file1.txt")
                zf.write("temp_files_for_zip_test/file2.txt", arcname="subdir/file2.txt") # Add a file in a subdir
            CommandLineDecompressor._logger.info(f"'{test_zip_archive}' created.")
            shutil.rmtree("temp_files_for_zip_test") # Clean up temporary files

        if os.path.exists(test_zip_archive):
            # Test decompression with overwrite
            print(f"\n--- Testing decompression of '{test_zip_archive}' with overwrite enabled ---")
            dummy_unzip_file_path = os.path.join(test_output_dir, "file1.txt")
            os.makedirs(test_output_dir, exist_ok=True)
            with open(dummy_unzip_file_path, "w") as f:
                f.write("This is a dummy file that should be overwritten by unzip.")
            CommandLineDecompressor._logger.info(f"Created dummy file: {dummy_unzip_file_path}")

            unzip_decompressor.decompress(test_zip_archive, test_output_dir)

            # Test listing contents
            print(f"\n--- Testing listing contents of '{test_zip_archive}' ---")
            contents = unzip_decompressor.list_contents(test_zip_archive)
            if contents:
                print(f"Contents of '{test_zip_archive}':")
                for item in contents:
                    print(f"  Name: {item.get('name')}, Size: {item.get('size')}, Date: {item.get('date_modified')}")
            else:
                print(f"Failed to list contents of '{test_zip_archive}'.")

            # Test archive integrity
            print(f"\n--- Testing integrity of '{test_zip_archive}' ---")
            unzip_decompressor.test_archive(test_zip_archive)
            
            # Clean up generated zip and output directory
            # os.remove(test_zip_archive)
            # shutil.rmtree(test_output_dir)
            # CommandLineDecompressor._logger.info("Cleaned up test files.")

        else:
            CommandLineDecompressor._logger.error(f"'{test_zip_archive}' not found even after attempt to create. Cannot proceed with tests.")
    else:
        CommandLineDecompressor._logger.error("UnZIPDecompressor is not available. Please ensure 'unzip' is installed and in your system's PATH.")
