import os
import re
import subprocess
import shutil
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.decompressor import Decompressor

# Assuming Decompressor base class is defined as in the previous turns

class UnZIPDecompressor(Decompressor):
    """
    A concrete implementation of the Decompressor abstract base class
    for handling ZIP archives using the 'unzip' command-line utility.

    During initialization, it attempts to find 'unzip' in the system's PATH
    and automatically sets its availability. If not found or verification fails,
    it remains unavailable until a specific path is provided via set_executable_path.
    """

    def __init__(self):
        """
        Initializes the UnZIPDecompressor.
        Attempts to find 'unzip' in the system's PATH upon instantiation
        and sets its initial availability status.
        """
        super().__init__()
        
        default_unzip_path = shutil.which("unzip")
        
        if default_unzip_path:
            self._log(logging.INFO, f"Attempting initial verification of 'unzip' from PATH: {default_unzip_path}")
            if self.set_executable_path(default_unzip_path):
                self._log(logging.INFO, "UnZIPDecompressor is initially available via system PATH.")
            else:
                self._log(logging.WARNING, f"Initial verification of '{default_unzip_path}' failed. UnZIPDecompressor is not available yet.")
        else:
            self._log(logging.INFO, "'unzip' command not found in system PATH during initialization. UnZIPDecompressor is not available yet.")
            self._is_available = False

    def _verify_executable(self, executable_path: str) -> bool:
        """
        Verifies if the given executable path points to a valid 'unzip' command.
        This is done by running 'unzip' with no arguments or a help flag,
        and checking for specific keywords in its output.
        :param executable_path: The path to the 'unzip' executable.
        :return: True if the executable is identified as 'unzip', False otherwise.
        """
        try:
            process = subprocess.run([executable_path, '-h'], capture_output=True, text=True, check=False, timeout=10)
            stdout_lower = process.stdout.lower()
            stderr_lower = process.stderr.lower()

            if "unzip" in stdout_lower and ("pkzip" in stdout_lower or "copyright" in stdout_lower or "usage" in stdout_lower):
                self._log(logging.DEBUG, f"Executable '{executable_path}' successfully verified as 'unzip'.")
                return True
            if "unzip" in stderr_lower and ("pkzip" in stderr_lower or "copyright" in stderr_lower or "usage" in stderr_lower):
                self._log(logging.DEBUG, f"Executable '{executable_path}' successfully verified as 'unzip' (output to stderr).")
                return True
            
            self._log(logging.INFO, f"Executable '{executable_path}' failed verification. Output did not match expected 'unzip' patterns.")
            self._log(logging.DEBUG, f"STDOUT:\n{process.stdout}")
            self._log(logging.DEBUG, f"STDERR:\n{process.stderr}")
            return False
        except (subprocess.CalledProcessError, FileNotFoundError, TimeoutError) as e:
            self._log(logging.DEBUG, f"Error during 'unzip' executable verification for '{executable_path}': {e}")
            return False
        except Exception as e:
            self._log(logging.DEBUG, f"An unknown error occurred during 'unzip' executable verification for '{executable_path}': {e}")
            return False

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None) -> bool:
        """
        Decompresses a ZIP archive using the 'unzip' command.
        Uses '-o' to automatically overwrite existing files without prompting.
        Uses '-d' to specify the destination directory.
        Treats exit codes 0 and 1 as successful decompression.
        :param archive_path: Path to the ZIP archive.
        :param output_path: Directory to extract files to. Will be created if it doesn't exist.
        :param password: Optional password for encrypted archives.
        :return: True on successful decompression, False otherwise.
        """
        self._check_availability()

        os.makedirs(output_path, exist_ok=True)

        command = [self.executable_path, '-o', archive_path, '-d', output_path]
        if password:
            command.extend([f'-P', password])

        self._log(logging.INFO, f"Executing unzip decompression command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0:
            self._log(logging.INFO, f"Successfully decompressed '{archive_path}' to '{output_path}'.")
            return True
        elif returncode == 1:
            self._log(logging.WARNING, f"Decompression of '{archive_path}' completed with minor issues (exit code 1). It may still be considered successful.")
            self._log(logging.DEBUG, f"Decompression STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return True
        else:
            self._log(logging.ERROR, f"Decompression of '{archive_path}' failed. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Decompression STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return False

    def list_contents(self, archive_path: str, password: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        Lists the contents of a ZIP archive using the 'unzip -l' command.
        Parses output to extract file information.
        Treats exit codes 0 and 1 as successful listing.
        Adjusted regex and parsing logic for the provided unzip -l output.
        :param archive_path: Path to the ZIP archive.
        :return: A list of dictionaries, each representing a file/directory
                 with 'name', 'size', 'date_modified', and 'time_modified'.
                 Returns None if listing fails.
        """
        self._check_availability()

        command = [self.executable_path, '-l', archive_path]
        if password:
            command.extend([f'-P', password])
        
        self._log(logging.INFO, f"Executing unzip list command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0 or returncode == 1: # Treat exit code 0 and 1 as success
            if returncode == 1:
                self._log(logging.WARNING, f"Listing of '{archive_path}' completed with minor issues (exit code 1). It may still be considered successful.")
                self._log(logging.DEBUG, f"Listing STDOUT:\n{stdout}\nSTDERR:\n{stderr}")

            contents = []
            # Updated regex:
            # - `\s*`: Allows for any amount of leading whitespace.
            # - `(?P<size>\d+)`: Captures the numeric size.
            # - `\s+`: One or more spaces.
            # - `(?P<date>\d{4}-\d{2}-\d{2}|\d{2}-\d{2}-\d{2})`: Captures date, allowing for both YYYY-MM-DD and MM-DD-YY formats.
            # - `\s+`: One or more spaces.
            # - `(?P<time>\d{2}:\d{2})`: Captures time.
            # - `\s+`: One or more spaces.
            # - `(?P<name>.*)`: Captures the rest of the line as the name (greedy, but since it's last, it works).
            
            # The format provided is `Length Date Time Name`
            # ` 361666 2022-12-30 20:58 sample-1/sample-1.webp`
            # The date format seems to be YYYY-MM-DD, not MM-DD-YY. Let's adjust for that primarily.
            # Re-evaluating based on "2022-12-31 04:43" format.
            file_pattern = re.compile(
                r'^\s*(?P<size>\d+)\s+'           # Size (digits)
                r'(?P<date>\d{4}-\d{2}-\d{2})\s+' # Date (YYYY-MM-DD)
                r'(?P<time>\d{2}:\d{2})\s+'       # Time (HH:MM)
                r'(?P<name>.*)$'                  # Name (rest of the line)
            )
            
            # This pattern matches the header line precisely, for robust detection
            header_pattern = re.compile(r'^\s*Length\s+Date\s+Time\s+Name\s*$')
            
            # This pattern matches the footer line precisely, for robust detection
            # It's flexible with spaces around the hyphens
            footer_dash_pattern = re.compile(r'^-+\s+-+\s+-+\s+-+\s*|\s*-+\s+-+\s*') 
            
            # This pattern matches the summary line at the very end
            # Example: 1098789 9 files
            summary_pattern = re.compile(r'^\s*\d+\s+.*?files$')

            in_content_section = False
            for line in stdout.splitlines():
                line_strip = line.strip()

                # Skip initial warning messages
                if not in_content_section and line_strip.lower().startswith(('warning', 'archive:')):
                    self._log(logging.DEBUG, f"Skipping initial warning/info line: '{line_strip}'")
                    continue
                
                # Detect the header line
                if header_pattern.match(line_strip):
                    self._log(logging.DEBUG, f"Detected header line: '{line_strip}'")
                    continue # Skip header line itself

                # Detect the first separator line (after header)
                if not in_content_section and footer_dash_pattern.match(line_strip):
                    self._log(logging.DEBUG, f"Detected start of content separator: '{line_strip}'")
                    in_content_section = True
                    continue # Skip this separator line

                # If we're in the content section and detect another separator or summary, it's the end
                if in_content_section and (footer_dash_pattern.match(line_strip) or summary_pattern.match(line_strip)):
                    self._log(logging.DEBUG, f"Detected end of content separator or summary: '{line_strip}'")
                    break # Stop processing lines

                # Process actual content lines
                if in_content_section and line_strip:
                    match = file_pattern.match(line)
                    if match:
                        item_data = match.groupdict()
                        name = item_data['name'].strip()
                        is_dir = name.endswith('/')
                        
                        contents.append({
                            'name': name,
                            'size': 'Directory' if is_dir else f"{item_data['size']} Bytes",
                            'date_modified': f"{item_data['date']} {item_data['time']}"
                            # 'attributes' not typically available directly from unzip -l in this format
                        })
                    else:
                        self._log(logging.DEBUG, f"Line in content section did not match file pattern: '{line_strip}'")

            self._log(logging.INFO, f"Successfully listed contents for '{archive_path}'. Found {len(contents)} items.")
            return contents
        else:
            self._log(logging.ERROR, f"Failed to list contents of '{archive_path}'. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Listing STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return None

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Tests the integrity of a ZIP archive using the 'unzip -t' command.
        Treats exit codes 0 and 1 as successful test.
        :param archive_path: Path to the ZIP archive.
        :param password: Optional password for encrypted archives.
        :return: True if the archive passes the test, False otherwise.
        """
        self._check_availability()

        command = [self.executable_path, '-t', archive_path]
        if password:
            command.extend(['-P', password])

        self._log(logging.INFO, f"Executing unzip test command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0:
            self._log(logging.INFO, f"Archive '{archive_path}' integrity test successful.")
            return True
        elif returncode == 1:
            self._log(logging.WARNING, f"Archive '{archive_path}' integrity test completed with minor issues (exit code 1). It may still be considered successful.")
            self._log(logging.DEBUG, f"Test STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return True
        else:
            self._log(logging.ERROR, f"Archive '{archive_path}' integrity test failed. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Test STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return False

    def get_supported_formats(self) -> List[str]:
        """
        Returns the list of compression formats supported by the 'unzip' utility.
        :return: A list of supported format strings (e.g., ['zip']).
        """
        super().get_supported_formats()
        self._log(logging.INFO, "Querying supported formats for UnZIPDecompressor.")
        return ['zip']


# --- Example Usage ---
if __name__ == "__main__":
    # Ensure logging is enabled for visibility
    Decompressor.enable_logging(True) 
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
            Decompressor._logger.info(f"Creating dummy '{test_zip_archive}' for testing...")
            os.makedirs("temp_files_for_zip_test", exist_ok=True)
            with open("temp_files_for_zip_test/file1.txt", "w") as f:
                f.write("This is file 1.")
            with open("temp_files_for_zip_test/file2.txt", "w") as f:
                f.write("This is file 2.")
            
            with zipfile.ZipFile(test_zip_archive, 'w') as zf:
                zf.write("temp_files_for_zip_test/file1.txt", arcname="file1.txt")
                zf.write("temp_files_for_zip_test/file2.txt", arcname="subdir/file2.txt") # Add a file in a subdir
            Decompressor._logger.info(f"'{test_zip_archive}' created.")
            shutil.rmtree("temp_files_for_zip_test") # Clean up temporary files

        if os.path.exists(test_zip_archive):
            # Test decompression with overwrite
            print(f"\n--- Testing decompression of '{test_zip_archive}' with overwrite enabled ---")
            dummy_unzip_file_path = os.path.join(test_output_dir, "file1.txt")
            os.makedirs(test_output_dir, exist_ok=True)
            with open(dummy_unzip_file_path, "w") as f:
                f.write("This is a dummy file that should be overwritten by unzip.")
            Decompressor._logger.info(f"Created dummy file: {dummy_unzip_file_path}")

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
            # Decompressor._logger.info("Cleaned up test files.")

        else:
            Decompressor._logger.error(f"'{test_zip_archive}' not found even after attempt to create. Cannot proceed with tests.")
    else:
        Decompressor._logger.error("UnZIPDecompressor is not available. Please ensure 'unzip' is installed and in your system's PATH.")
