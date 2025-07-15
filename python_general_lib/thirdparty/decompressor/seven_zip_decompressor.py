import os
import re
import subprocess
import shutil
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.decompressor import Decompressor

# Assuming Decompressor base class is defined as in the previous turns

class SevenZipDecompressor(Decompressor):
    """
    A concrete implementation of the Decompressor abstract base class
    for handling 7-Zip archives and others using the '7z' command-line utility.

    During initialization, it attempts to find '7z' in the system's PATH
    and automatically sets its availability. If not found or verification fails,
    it remains unavailable until a specific path is provided via set_executable_path.
    """

    def __init__(self):
        """
        Initializes the SevenZipDecompressor.
        Attempts to find '7z' in the system's PATH upon instantiation
        and sets its initial availability status.
        """
        super().__init__()
        
        default_7z_path = shutil.which("7z")
        if not default_7z_path:
            default_7z_path = shutil.which("7za")
            if default_7z_path:
                self._log(logging.INFO, "Using '7za' as '7z' was not found in PATH.")
        
        if default_7z_path:
            self._log(logging.INFO, f"Attempting initial verification of '7z' from PATH: {default_7z_path}")
            if self.set_executable_path(default_7z_path):
                self._log(logging.INFO, "SevenZipDecompressor is initially available via system PATH.")
            else:
                self._log(logging.WARNING, f"Initial verification of '{default_7z_path}' failed. SevenZipDecompressor is not available yet.")
        else:
            self._log(logging.INFO, "'7z' or '7za' command not found in system PATH during initialization. SevenZipDecompressor is not available yet.")
            self._is_available = False

    def _verify_executable(self, executable_path: str) -> bool:
        """
        Verifies if the given executable path points to a valid '7z' command.
        This is done by running '7z' with no arguments, which typically prints
        its usage information or version.
        :param executable_path: The path to the '7z' executable.
        :return: True if the executable is identified as '7z', False otherwise.
        """
        try:
            process = subprocess.run([executable_path], capture_output=True, text=True, check=False, timeout=10)
            stdout_lower = process.stdout.lower()
            stderr_lower = process.stderr.lower()

            if "7-zip" in stdout_lower and ("copyright" in stdout_lower or "usage" in stdout_lower or "7z.exe" in stdout_lower):
                self._log(logging.DEBUG, f"Executable '{executable_path}' successfully verified as '7z'.")
                return True
            if "7-zip" in stderr_lower and ("copyright" in stderr_lower or "usage" in stderr_lower or "7z.exe" in stderr_lower):
                self._log(logging.DEBUG, f"Executable '{executable_path}' successfully verified as '7z' (output to stderr).")
                return True
            
            self._log(logging.INFO, f"Executable '{executable_path}' failed verification. Output did not match expected '7z' patterns.")
            self._log(logging.DEBUG, f"STDOUT:\n{process.stdout}")
            self._log(logging.DEBUG, f"STDERR:\n{process.stderr}")
            return False
        except (subprocess.CalledProcessError, FileNotFoundError, TimeoutError) as e:
            self._log(logging.DEBUG, f"Error during '7z' executable verification for '{executable_path}': {e}")
            return False
        except Exception as e:
            self._log(logging.DEBUG, f"An unknown error occurred during '7z' executable verification for '{executable_path}': {e}")
            return False

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None) -> bool:
        """
        Decompresses an archive using the '7z x' command (extract with full paths).
        Uses '-aoa' to automatically overwrite existing files without prompting.
        :param archive_path: Path to the archive.
        :param output_path: Directory to extract files to. Will be created if it doesn't exist.
        :param password: Optional password for encrypted archives.
        :return: True on successful decompression, False otherwise.
        """
        self._check_availability()

        os.makedirs(output_path, exist_ok=True)

        command = [self.executable_path, 'x', archive_path, f'-o{output_path}', '-aoa']
        if password:
            command.append(f'-p{password}')

        self._log(logging.INFO, f"Executing 7z decompression command for '{archive_path}' to '{output_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0:
            self._log(logging.INFO, f"Successfully decompressed '{archive_path}' to '{output_path}'.")
            return True
        elif returncode == 1:
            self._log(logging.WARNING, f"Decompression of '{archive_path}' completed with minor issues (exit code 1). Review logs for details.")
            self._log(logging.DEBUG, f"Decompression STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return True
        else:
            self._log(logging.ERROR, f"Decompression of '{archive_path}' failed. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Decompression STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return False

    def list_contents(self, archive_path: str) -> Optional[List[Dict[str, str]]]:
        """
        Lists the contents of an archive using the '7z l' command.
        Parses output to extract file information.
        Adjusted regex and parsing logic for the specific 7z -l output format provided.
        :param archive_path: Path to the archive.
        :return: A list of dictionaries, each representing a file/directory
                 with 'name', 'size', 'date_modified', and 'attributes'.
                 Returns None if listing fails.
        """
        self._check_availability()

        command = [self.executable_path, 'l', archive_path]
        self._log(logging.INFO, f"Executing 7z list command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0 or returncode == 1:
            if returncode == 1:
                self._log(logging.WARNING, f"Listing of '{archive_path}' completed with minor issues (exit code 1). Review logs for details.")
                self._log(logging.DEBUG, f"Listing STDOUT:\n{stdout}\nSTDERR:\n{stderr}")

            contents = []
            # Updated regex specifically for the end columns (Compressed and Name)
            file_pattern = re.compile(
                r'^(?P<date>\d{4}-\d{2}-\d{2})\s+'    # Date (YYYY-MM-DD)
                r'(?P<time>\d{2}:\d{2}:\d{2})\s+'     # Time (HH:MM:SS)
                r'(?P<attributes>[D.RHAESULC]{5})\s+' # Attributes (e.g., D...., ....A, exactly 5 chars)
                r'(?P<size>\d+)\s+'                   # Size (uncompressed)
                r'(?P<compressed>\s*\d*\s*)\s*'      # Compressed size (optional digits, surrounded by spaces)
                r'(?P<name>.*)$'                      # Name (rest of line after compressed size and separating spaces)
            )
            
            header_column_names_pattern = re.compile(r'^\s*Date\s+Time\s+Attr\s+Size\s+Compressed\s+Name\s*$')
            separator_pattern = re.compile(r'^-+\s*-+\s*-+\s*-+\s*-+\s*-+\s*$')
            summary_line_pattern = re.compile(r'^\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+(?:\s*\d+){1,2}\s+\d+\s+files,\s+\d+\s+folders$')

            found_header_columns = False
            in_content_section = False
            
            for line in stdout.splitlines():
                line_strip = line.strip()

                # Filter out lines that are definitely not part of the content table
                # (Removed '--' from here as it's a structural separator that needs regex matching)
                if not line_strip or \
                   line_strip.startswith(('7-Zip', '64-bit locale', 'Scanning the drive', '1 file,', 'Listing archive:', 'Path =', 'Type =', 'Physical Size =', 'Headers Size =', 'Method =', 'Solid =', 'Blocks =')):
                    self._log(logging.DEBUG, f"Skipping non-content info line: '{line_strip}'")
                    continue
                
                # Step 1: Find the line with column names
                if not found_header_columns and header_column_names_pattern.match(line_strip):
                    self._log(logging.DEBUG, f"Detected column header line: '{line_strip}'")
                    found_header_columns = True
                    continue

                # Step 2: Find the separator line after column names (marks content start)
                if found_header_columns and not in_content_section and separator_pattern.match(line_strip):
                    self._log(logging.DEBUG, f"Detected start of content separator line: '{line_strip}'")
                    in_content_section = True
                    continue

                # Step 3: Once in content section, detect end conditions (another separator or summary line)
                if in_content_section and (separator_pattern.match(line_strip) or summary_line_pattern.match(line_strip)):
                    self._log(logging.DEBUG, f"Detected end of content (footer separator or summary): '{line_strip}'")
                    break

                # Step 4: Process actual content lines within the content section
                if in_content_section and line_strip:
                    match = file_pattern.match(line)
                    if match:
                        item_data = match.groupdict()
                        name = item_data['name'].strip()
                        
                        is_dir = 'D' in item_data['attributes']
                        
                        contents.append({
                            'name': name,
                            'size': 'Directory' if is_dir else f"{item_data['size']} Bytes",
                            'date_modified': f"{item_data['date']} {item_data['time']}",
                            'attributes': item_data['attributes']
                        })
                    else:
                        self._log(logging.DEBUG, f"Line in content section did not match 7z file pattern: '{line}'")

            self._log(logging.INFO, f"Successfully listed contents for '{archive_path}'. Found {len(contents)} items.")
            return contents
        else:
            self._log(logging.ERROR, f"Failed to list contents of '{archive_path}'. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Listing STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return None

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Tests the integrity of an archive using the '7z t' command.
        :param archive_path: Path to the archive.
        :param password: Optional password for encrypted archives.
        :return: True if the archive passes the test, False otherwise.
        """
        self._check_availability()

        command = [self.executable_path, 't', archive_path]
        if password:
            command.append(f'-p{password}')

        self._log(logging.INFO, f"Executing 7z test command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0:
            self._log(logging.INFO, f"Archive '{archive_path}' integrity test successful.")
            return True
        elif returncode == 1:
            self._log(logging.WARNING, f"Archive '{archive_path}' integrity test completed with minor issues (exit code 1). Review logs for details.")
            self._log(logging.DEBUG, f"Test STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return True
        else:
            self._log(logging.ERROR, f"Archive '{archive_path}' integrity test failed. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Test STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return False

    def get_supported_formats(self) -> List[str]:
        """
        Returns the list of compression formats supported by the '7z' utility.
        This utility supports a very wide range of formats.
        :return: A list of supported format strings.
        """
        self._check_availability()
        self._log(logging.INFO, "Querying supported formats for SevenZipDecompressor.")
        return ['7z', 'zip', 'rar', 'tar', 'gzip', 'bzip2', 'xz', 'iso', 'arj', 'lzh', 'chm', 'nsis', 'rpm', 'deb']



# --- Example Usage ---
if __name__ == "__main__":
    Decompressor.enable_logging(True) 
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
            Decompressor._logger.warning(f"'{test_7z_archive}' not found. Attempting to create it using 7z command...")
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
                
                Decompressor._logger.info(f"Executing 7z creation command: {' '.join(create_command)}")
                creation_process = subprocess.run(create_command, capture_output=True, text=True, check=False)
                if creation_process.returncode == 0:
                    Decompressor._logger.info(f"Successfully created '{test_7z_archive}'.")
                else:
                    Decompressor._logger.error(f"Failed to create '{test_7z_archive}'. Exit code: {creation_process.returncode}\nSTDOUT:\n{creation_process.stdout}\nSTDERR:\n{creation_process.stderr}")
            else:
                Decompressor._logger.error("7z executable not available to create test archive. Please create one manually or ensure 7z is installed.")
            shutil.rmtree("temp_files_for_7z_test") # Clean up source files

        if os.path.exists(test_7z_archive):
            # Test decompression with overwrite
            print(f"\n--- Testing decompression of '{test_7z_archive}' with overwrite enabled ---")
            dummy_7z_file_path = os.path.join(test_output_dir, "fileA.txt")
            os.makedirs(test_output_dir, exist_ok=True)
            with open(dummy_7z_file_path, "w") as f:
                f.write("This is a dummy file that should be overwritten by 7z.")
            Decompressor._logger.info(f"Created dummy file: {dummy_7z_file_path}")

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
            # Decompressor._logger.info("Cleaned up test files.")

        else:
            Decompressor._logger.error(f"'{test_7z_archive}' not found. Skipping 7z archive tests.")
        
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
            Decompressor._logger.warning(f"'{test_unzip_archive_name}' not found. Skipping 7z test with zip archive.")
            

    else:
        Decompressor._logger.error("SevenZipDecompressor is not available. Please ensure '7z' (or '7za') is installed and in your system's PATH.")
