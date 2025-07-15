import os
import re
import subprocess
import shutil # For shutil.which() to find executables in PATH
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.decompressor import Decompressor

# Assuming Decompressor base class is defined as in the previous turns

class UnRARDecompressor(Decompressor):
    """
    A concrete implementation of the Decompressor abstract base class
    for handling RAR archives using the 'unrar' command-line utility.

    During initialization, it attempts to find 'unrar' in the system's PATH
    and automatically sets its availability. If not found or verification fails,
    it remains unavailable until a specific path is provided via set_executable_path.
    """

    def __init__(self):
        """
        Initializes the UnRARDecompressor.
        Attempts to find 'unrar' in the system's PATH upon instantiation
        and sets its initial availability status.
        """
        super().__init__()
        
        default_unrar_path = shutil.which("unrar")
        
        if default_unrar_path:
            self._log(logging.INFO, f"Attempting initial verification of 'unrar' from PATH: {default_unrar_path}")
            if self.set_executable_path(default_unrar_path):
                self._log(logging.INFO, "UnRARDecompressor is initially available via system PATH.")
            else:
                self._log(logging.WARNING, f"Initial verification of '{default_unrar_path}' failed. UnRARDecompressor is not available yet.")
        else:
            self._log(logging.INFO, "'unrar' command not found in system PATH during initialization. UnRARDecompressor is not available yet.")
            self._is_available = False

    def _verify_executable(self, executable_path: str) -> bool:
        """
        Verifies if the given executable path points to a valid 'unrar' command.
        This is done by running 'unrar' with no arguments, which typically prints
        its usage information or version, and checking for specific keywords.
        :param executable_path: The path to the 'unrar' executable.
        :return: True if the executable is identified as 'unrar', False otherwise.
        """
        try:
            process = subprocess.run([executable_path], capture_output=True, text=True, check=False, timeout=10)
            stdout_lower = process.stdout.lower()
            stderr_lower = process.stderr.lower()

            if "unrar" in stdout_lower and ("rar archive" in stdout_lower or "copyright" in stdout_lower or "usage" in stdout_lower or "unrar.exe" in stdout_lower):
                self._log(logging.DEBUG, f"Executable '{executable_path}' successfully verified as 'unrar'.")
                return True
            if "unrar" in stderr_lower and ("rar archive" in stderr_lower or "copyright" in stderr_lower or "usage" in stderr_lower or "unrar.exe" in stderr_lower):
                self._log(logging.DEBUG, f"Executable '{executable_path}' successfully verified as 'unrar' (output to stderr).")
                return True
            
            self._log(logging.INFO, f"Executable '{executable_path}' failed verification. Output did not match expected 'unrar' patterns.")
            self._log(logging.DEBUG, f"STDOUT:\n{process.stdout}")
            self._log(logging.DEBUG, f"STDERR:\n{process.stderr}")
            return False
        except (subprocess.CalledProcessError, FileNotFoundError, TimeoutError) as e:
            self._log(logging.DEBUG, f"Error during 'unrar' executable verification for '{executable_path}': {e}")
            return False
        except Exception as e:
            self._log(logging.DEBUG, f"An unknown error occurred during 'unrar' executable verification for '{executable_path}': {e}")
            return False

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None) -> bool:
        """
        Decompresses a RAR archive using the 'unrar x' command.
        The 'x' command extracts files with full paths.
        Uses '-o+' to automatically overwrite existing files without prompting.
        :param archive_path: Path to the RAR archive.
        :param output_path: Directory to extract files to. Will be created if it doesn't exist.
        :param password: Optional password for encrypted archives.
        :return: True on successful decompression, False otherwise.
        """
        self._check_availability() # Ensure the decompressor is available

        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Added '-o+' switch to overwrite existing files without prompting
        command = [self.executable_path, 'x', archive_path, output_path, '-o+'] 
        if password:
            command.extend([f'-p{password}']) # -p<password> for password

        self._log(logging.INFO, f"Executing unrar decompression command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        # 'unrar' returns 0 on success.
        if returncode == 0:
            self._log(logging.INFO, f"Successfully decompressed '{archive_path}' to '{output_path}'.")
            return True
        else:
            self._log(logging.ERROR, f"Decompression of '{archive_path}' failed. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Decompression STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return False

    def list_contents(self, archive_path: str) -> Optional[List[Dict[str, str]]]:
        """
        Lists the contents of a RAR archive using the 'unrar l' command.
        Adjusted regex to match the provided output format.
        :param archive_path: Path to the RAR archive.
        :return: A list of dictionaries, each representing a file/directory
                 with 'name' and 'size' (and potentially 'date', 'attr').
                 Returns None if listing fails.
        """
        self._check_availability() # Ensure the decompressor is available

        command = [self.executable_path, 'l', archive_path]
        self._log(logging.INFO, f"Executing unrar list command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0:
            contents = []
            # Updated regex to match the specific 'unrar 7.00' output format:
            # Attributes      Size      Date    Time    Name
            # ----------- ---------  ---------- -----  ----
            #  -rw-r--r--    361666  2022-12-31 04:50  sample-1_1.webp
            # ----------- ---------  ---------- -----  ----
            #               361666                      1 (Summary line, ignore)
            
            # This regex captures Attributes, Size, Date, Time, and Name.
            # It uses flexible whitespace matching (\s+) and specific column structures.
            file_pattern = re.compile(
                r'^\s*(?P<attributes>[a-zA-Z-]+)\s+' # Attributes (e.g., -rw-r--r--)
                r'(?P<size>\d+)\s+'                  # Size (digits)
                r'(?P<date>\d{4}-\d{2}-\d{2})\s+'    # Date (YYYY-MM-DD)
                r'(?P<time>\d{2}:\d{2})\s+'          # Time (HH:MM)
                r'(?P<name>.*)$'                     # Name (rest of the line, non-greedy)
            )
            
            in_content_section = False
            for line in stdout.splitlines():
                line_strip = line.strip()
                if line_strip.startswith('-----------'): # Start or end of content section
                    if in_content_section: # Second '-----------' indicates end
                        break
                    else: # First '-----------' indicates start of content
                        in_content_section = True
                        continue # Skip the header line itself

                if in_content_section and line_strip: # Process content lines
                    match = file_pattern.match(line)
                    if match:
                        item_data = match.groupdict()
                        contents.append({
                            'name': item_data['name'].strip(),
                            'size': item_data['size'] + ' Bytes', # Add unit for clarity
                            'date_modified': f"{item_data['date']} {item_data['time']}",
                            'attributes': item_data['attributes']
                        })
                    # This specific 'unrar 7.00' output doesn't seem to have explicit directory lines ending with '/'.
                    # Directories are usually just listed without size/date/time.
                    # We might need a separate pattern if directories are formatted differently.
                    # Based on your example, it only shows a file.
            self._log(logging.INFO, f"Successfully listed contents for '{archive_path}'. Found {len(contents)} items.")
            return contents
        else:
            self._log(logging.ERROR, f"Failed to list contents of '{archive_path}'. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Listing STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return None

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Tests the integrity of a RAR archive using the 'unrar t' command.
        :param archive_path: Path to the RAR archive.
        :param password: Optional password.
        :return: True if the archive passes the test, False otherwise.
        """
        super().test_archive(archive_path, password)

        command = [self.executable_path, 't', archive_path]
        if password:
            command.extend([f'-p{password}'])

        self._log(logging.INFO, f"Executing unrar test command for '{archive_path}'")
        returncode, stdout, stderr = self._execute_command(command)

        if returncode == 0:
            self._log(logging.INFO, f"Archive '{archive_path}' integrity test successful.")
            return True
        else:
            self._log(logging.ERROR, f"Archive '{archive_path}' integrity test failed. Exit code: {returncode}")
            self._log(logging.DEBUG, f"Test STDOUT:\n{stdout}\nSTDERR:\n{stderr}")
            return False

    def get_supported_formats(self) -> List[str]:
        """
        Returns the list of compression formats supported by the 'unrar' utility.
        :return: A list of supported format strings (e.g., ['rar']).
        """
        super().get_supported_formats()
        self._log(logging.INFO, "Querying supported formats for UnRARDecompressor.")
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