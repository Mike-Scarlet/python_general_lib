import os
import re
import subprocess
import shutil
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.command_line_decompressor import CommandLineDecompressor

# Assuming Decompressor base class is defined as in the previous turns

class SevenZipDecompressor(CommandLineDecompressor):
    """
    Implementation for 7-Zip decompression using 7z command-line utility.
    Supports listing, decompressing and testing various archive formats.
    """
    
    def __init__(self):
        """
        Initialize the 7-Zip decompressor.
        Attempts to find '7z' or '7za' in system PATH and verifies it.
        """
        super().__init__(
            executable_names=["7z", "7za"],
            verification_keywords=["7-zip", "copyright", "usage"]
        )

    def add_password_to_command(self, command, password=None):
        if password:
            command.append(f'-p{password}')
        else:
            command.append(f'-p""')

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None, extra_switch: Optional[list[str]] = None) -> bool:
        """
        Decompress an archive using 7z
        
        :param archive_path: Path to archive file
        :param output_path: Output directory
        :param password: Optional password
        :return: True on success, False on failure
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        # Build decompression command
        command = [
            self._executable_path,
            'x',  # Extract with full paths
            archive_path,
            f'-o{output_path}',  # Output directory
            '-aoa'  # Overwrite all existing files without prompt
        ]

        if extra_switch:
            command.extend(extra_switch)
        
        # Add password if provided
        self.add_password_to_command(command, password)
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Check result (7z returns 0 for success, 1 for warnings)
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
        List contents of an archive using 7z
        
        :param archive_path: Path to archive file
        :param password: Optional password
        :return: List of contents or None on failure
        """
        # Build list command
        command = [self._executable_path, 'l', archive_path]
        self.add_password_to_command(command, password)
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Check command success (7z returns 0 for success, 1 for warnings)
        if returncode not in (0, 1):
            self._log(logging.ERROR, f"Listing failed for '{archive_path}', error code: {returncode}")
            self._log(logging.DEBUG, f"STDOUT: {stdout}")
            self._log(logging.DEBUG, f"STDERR: {stderr}")
            return None
            
        # Define parsing rules for 7z output
        parse_rules = {
            'header_pattern': re.compile(r'Date\s+Time\s+Attr\s+Size\s+Compressed\s+Name'),
            'header_separator_pattern': re.compile(r'^(?:-+\s+){3,}-+$'),
            'row_pattern': re.compile(
                r'^(?P<date>\d{4}-\d{2}-\d{2})\s+'  # Date (YYYY-MM-DD)
                r'(?P<time>\d{2}:\d{2}:\d{2})\s+'  # Time (HH:MM:SS)
                r'(?P<attributes>[D\.A-Z]{5})\s+'  # Attributes (5 characters)
                r'(?P<size>\d+)\s+'                 # Size (digits)
                r'(?P<compressed>\d*)\s+'           # Compressed size (optional digits)
                r'(?P<name>.*)$'                    # Name (rest of line)
            ),
            'footer_pattern': re.compile(r'^(?:-+\s+){3,}-+$'),
            'skip_patterns': [
                r'^7-Zip\s+\[',          # Skip version line
                r'^Scanning\s+the\s+drive', # Skip scanning message
                r'^Path\s+=\s+',          # Skip path info
                r'^Type\s+=\s+',          # Skip type info
                r'^Physical\s+Size\s+=\s+', # Skip physical size
                r'^Headers\s+Size\s+=\s+', # Skip headers size
                r'^Method\s+=\s+',       # Skip method info
                r'^Solid\s+=\s+',         # Skip solid info
                r'^Blocks\s+=\s+',        # Skip blocks info
                r'^Comment\s+=\s+',        # Skip comment info
                r'^$',                    # Skip empty lines
            ],
            'is_dir_indicator': 'D',      # 'D' in attributes indicates directory
            'size_unit': ' Bytes',
        }
        
        # Parse output content
        contents = self._parse_list_output(stdout, parse_rules)
        self._log(logging.INFO, f"Successfully listed '{archive_path}', found {len(contents)} items")
        return contents

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Test integrity of an archive using 7z
        
        :param archive_path: Path to archive file
        :param password: Optional password
        :return: True on success, False on failure
        """
        # Build test command
        command = [self._executable_path, 't', archive_path]
        self.add_password_to_command(command, password)
            
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Check result (7z returns 0 for success, 1 for warnings)
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
        
        :return: List of supported formats
        """
        self._log(logging.INFO, "Querying supported formats for 7-Zip")
        
        # Build command to get supported formats
        command = [self._executable_path, 'i']
        
        # Execute command
        returncode, stdout, stderr = self._execute_command(command)
        
        # Parse supported formats from output
        formats = []
        if returncode == 0:
            # Look for lines like: "Formats: 7z XZ..."
            format_line = re.search(r'Formats:\s+(.*)', stdout)
            if format_line:
                formats = format_line.group(1).split()
        
        # Fallback to common formats if parsing fails
        if not formats:
            formats = ['7z', 'zip', 'rar', 'tar', 'gzip', 'bzip2', 'xz', 'iso']
            
        self._log(logging.INFO, f"Supported formats: {', '.join(formats)}")
        return formats



# --- Example Usage ---
if __name__ == "__main__":
    CommandLineDecompressor.enable_logging(True) 
    print("--- Testing SevenZipDecompressor ---")

    sevenzip_decompressor = SevenZipDecompressor()
    print(f"Initial availability: {sevenzip_decompressor.is_available()}")

    if sevenzip_decompressor.is_available():
        test_7z_archive = "test_archive.7z" # Create this file for testing
        test_zip_archive_7z = "test_archive.zip" # Can also use 7z to handle zip
        test_output_dir = "7z_output"

        # --- Create a dummy .7z archive for demonstration ---
        # Note: Python's standard library does not have a native .7z creator.
        # You would typically create this manually with 7-Zip GUI/CLI.
        # For programmatic creation, you'd call 7z itself.
        if not os.path.exists(test_7z_archive):
            CommandLineDecompressor._logger.warning(f"'{test_7z_archive}' not found. Attempting to create it using 7z command...")
            # This requires '7z' to be in PATH even for creation
            if sevenzip_decompressor.is_available():
                os.makedirs("temp_files_for_7z_test", exist_ok=True)
                with open("temp_files_for_7z_test/fileA.txt", "w") as f:
                    f.write("Content of file A.")
                with open("temp_files_for_7z_test/fileB.txt", "w") as f:
                    f.write("Content of file B.")
                os.makedirs("temp_files_for_7z_test/subdir_7z", exist_ok=True)
                with open("temp_files_for_7z_test/subdir_7z/fileC.txt", "w") as f:
                    f.write("Content of file C in subdir.")
                
                create_command = [
                    sevenzip_decompressor.executable_path, 'a', '-t7z', 
                    test_7z_archive, 'temp_files_for_7z_test/'
                ]
                # If password needed: create_command.append('-pYOUR_PASSWORD')
                
                CommandLineDecompressor._logger.info(f"Executing 7z creation command: {' '.join(create_command)}")
                creation_process = subprocess.run(create_command, capture_output=True, text=True, check=False)
                if creation_process.returncode == 0:
                    CommandLineDecompressor._logger.info(f"Successfully created '{test_7z_archive}'.")
                else:
                    CommandLineDecompressor._logger.error(f"Failed to create '{test_7z_archive}'. Exit code: {creation_process.returncode}\nSTDOUT:\n{creation_process.stdout}\nSTDERR:\n{creation_process.stderr}")
            else:
                CommandLineDecompressor._logger.error("7z executable not available to create test archive. Please create one manually or ensure 7z is installed.")
            shutil.rmtree("temp_files_for_7z_test") # Clean up source files

        if os.path.exists(test_7z_archive):
            # Test decompression with overwrite
            print(f"\n--- Testing decompression of '{test_7z_archive}' with overwrite enabled ---")
            dummy_7z_file_path = os.path.join(test_output_dir, "fileA.txt")
            os.makedirs(test_output_dir, exist_ok=True)
            with open(dummy_7z_file_path, "w") as f:
                f.write("This is a dummy file that should be overwritten by 7z.")
            CommandLineDecompressor._logger.info(f"Created dummy file: {dummy_7z_file_path}")

            sevenzip_decompressor.decompress(test_7z_archive, test_output_dir)

            # Test listing contents
            print(f"\n--- Testing listing contents of '{test_7z_archive}' ---")
            contents = sevenzip_decompressor.list_contents(test_7z_archive)
            if contents:
                print(f"Contents of '{test_7z_archive}':")
                for item in contents:
                    print(f"  Name: {item.get('name')}, Size: {item.get('size')}, Date: {item.get('date_modified')}, Attr: {item.get('attributes')}")
            else:
                print(f"Failed to list contents of '{test_7z_archive}'.")

            # Test archive integrity
            print(f"\n--- Testing integrity of '{test_7z_archive}' ---")
            sevenzip_decompressor.test_archive(test_7z_archive)
            
            # Clean up generated 7z archive and output directory
            # if os.path.exists(test_7z_archive): os.remove(test_7z_archive)
            # if os.path.exists(test_output_dir): shutil.rmtree(test_output_dir)
            # CommandLineDecompressor._logger.info("Cleaned up test files.")

        else:
            CommandLineDecompressor._logger.error(f"'{test_7z_archive}' not found. Skipping 7z archive tests.")
        
        # --- Test 7z handling a ZIP file ---
        # You'd need a test_archive.zip for this to work
        test_unzip_archive_name = "test_archive.zip" # Re-use the name from unzip test if it exists
        test_unzip_output_dir_7z = "7z_output_zip"
        if os.path.exists(test_unzip_archive_name):
            print(f"\n--- Testing 7z decompression of a ZIP archive ('{test_unzip_archive_name}') ---")
            os.makedirs(test_unzip_output_dir_7z, exist_ok=True)
            sevenzip_decompressor.decompress(test_unzip_archive_name, test_unzip_output_dir_7z)
            # shutil.rmtree(test_unzip_output_dir_7z) # Clean up
        else:
            CommandLineDecompressor._logger.warning(f"'{test_unzip_archive_name}' not found. Skipping 7z test with zip archive.")
            

    else:
        CommandLineDecompressor._logger.error("SevenZipDecompressor is not available. Please ensure '7z' (or '7za') is installed and in your system's PATH.")
