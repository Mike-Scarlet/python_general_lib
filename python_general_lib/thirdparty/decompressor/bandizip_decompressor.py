
import os
import re
import subprocess
import shutil
import logging
from typing import List, Dict, Optional
from python_general_lib.thirdparty.decompressor.command_line_decompressor import CommandLineDecompressor

class BandizipDecompressor(Decompressor):
    def __init__(self):
        super().__init__()
        self._log(logging.INFO, "BandizipDecompressor instance created.")

    def _verify_executable(self, executable_path: str) -> bool:
        """
        Verifies if the provided executable path points to a valid Bandizip command-line executable (bz.exe).
        It does this by checking the filename and attempting to get its version.
        """
        basename = os.path.basename(executable_path).lower()
        if basename != "bz.exe":
            self._log(logging.WARNING, f"Executable name '{basename}' does not match expected Bandizip command-line executable name (bz.exe).")
            return False

        # Attempt to get version information by executing bz.exe without arguments
        # As per the help, bz.exe itself outputs version and usage info when run without args.
        command = [executable_path] 
        
        # Use the special helper for verification to avoid availability check recursion
        return_code, stdout, stderr = self._execute_command_without_availability_check(command) 
        
        # Check for typical Bandizip version output like "bz 7.36(Beta,x64) - Bandizip console tool."
        # A return code of 0 and the presence of "Bandizip console tool." indicates success.
        if return_code == 0 and "Bandizip console tool." in stdout:
            self._log(logging.INFO, f"Bandizip executable '{executable_path}' verified successfully.")
            return True
        else:
            self._log(logging.WARNING, f"Verification failed for Bandizip executable '{executable_path}'. Return code: {return_code}, Stdout: {stdout.strip()}, Stderr: {stderr.strip()}")
            return False

    # A special helper for _verify_executable to avoid recursive _check_availability calls
    def _execute_command_without_availability_check(self, command: List[str]) -> tuple[int, str, str]:
        """
        Executes a command-line instruction without checking the decompressor's availability.
        Used specifically by _verify_executable.
        """
        try:
            self._log(logging.DEBUG, f"Executing verification command: {' '.join(command)}")
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            return process.returncode, process.stdout, process.stderr
        except FileNotFoundError:
            self._log(logging.ERROR, f"Executable not found during verification: {command[0]}")
            return -1, "", "Executable not found."
        except Exception as e:
            self._log(logging.ERROR, f"An error occurred during verification command '{' '.join(command)}': {e}")
            return -1, "", str(e)

    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None, extra_switch: Optional[list[str]] = None) -> bool:
        """
        Decompresses an archive using Bandizip's bz.exe.
        The command format is generally: bz.exe x [archive_path] -o:[output_path] -p:[password]
        """
        super().decompress(archive_path, output_path, password) # Call super method for availability check

        if not os.path.exists(archive_path):
            self._log(logging.ERROR, f"Archive not found: {archive_path}")
            return False

        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        command = [self.executable_path, "x", archive_path, f"-o:{output_path}"]
        if password:
            command.append(f"-p:{password}")

        return_code, stdout, stderr = self._execute_command(command)

        if return_code == 0:
            self._log(logging.INFO, f"Successfully decompressed '{archive_path}' to '{output_path}'.")
            return True
        else:
            self._log(logging.ERROR, f"Failed to decompress '{archive_path}'. Stdout: {stdout.strip()}, Stderr: {stderr.strip()}")
            return False

    def list_contents(self, archive_path: str, password: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        Lists the contents of an archive using Bandizip's bz.exe.
        The command format is generally: bz.exe l [archive_path] -p:[password]
        Parses the output to return a list of dictionaries.
        """
        super().list_contents(archive_path, password) # Call super method for availability check

        if not os.path.exists(archive_path):
            self._log(logging.ERROR, f"Archive not found for listing: {archive_path}")
            return None

        command = [self.executable_path, "l", archive_path]
        if password:
            command.append(f"-p:{password}")

        return_code, stdout, stderr = self._execute_command(command)

        if return_code == 0:
            contents = []
            # bz.exe output for listing typically has a header followed by file details.
            # Example:
            # Type      Size     Packed    Date        Time      Name
            # -------- --------- -------- ----------- -------- -----------------------------------
            # File        1024      512 2023-01-01 10:00:00 file1.txt
            # Dir            0        0 2023-01-01 10:05:00 folder/
            # File       20480    10240 2023-01-01 10:15:00 folder/file2.txt

            lines = stdout.splitlines()
            header_index = -1
            for i, line in enumerate(lines):
                # Look for the separator line to find the start of content
                if "--------" in line and "Name" in line: 
                    header_index = i
                    break

            if header_index == -1 or header_index + 1 >= len(lines):
                self._log(logging.WARNING, f"Could not parse Bandizip list output for '{archive_path}'. Output:\n{stdout}")
                return None

            for line in lines[header_index + 1:]:
                stripped_line = line.strip()
                if not stripped_line or stripped_line.startswith("Total"):
                    continue # Skip empty lines or summary/total lines

                # Use regex for more robust parsing, as space splitting can be tricky with varying lengths
                # Regex to capture Type, Size, Packed, Date, Time, Name (and anything after Name)
                # The regex captures everything after "Time" as the name, then we strip it.
                match = re.match(r"(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)", stripped_line)
                if match:
                    file_type, size, packed, date, time, name_full = match.groups()
                    name = name_full.strip() # Strip whitespace from the name
                    contents.append({
                        "name": name, 
                        "type": file_type, 
                        "size": size,
                        "packed_size": packed, 
                        "date": date,
                        "time": time
                    })
                else:
                    self._log(logging.DEBUG, f"Skipping unparseable line during list_contents: {stripped_line}")

            self._log(logging.INFO, f"Successfully listed contents of '{archive_path}'. Found {len(contents)} items.")
            return contents
        else:
            self._log(logging.ERROR, f"Failed to list contents of '{archive_path}'. Stdout: {stdout.strip()}, Stderr: {stderr.strip()}")
            return None

    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Tests the integrity of an archive using Bandizip's bz.exe.
        The command format is generally: bz.exe t [archive_path] -p:[password]
        """
        super().test_archive(archive_path, password) # Call super method for availability check

        if not os.path.exists(archive_path):
            self._log(logging.ERROR, f"Archive not found for testing: {archive_path}")
            return False

        command = [self.executable_path, "t", archive_path]
        if password:
            command.append(f"-p:{password}")

        return_code, stdout, stderr = self._execute_command(command)

        # bz.exe typically indicates success with "No errors" in stdout
        if return_code == 0 and "No errors" in stdout: 
            self._log(logging.INFO, f"Archive '{archive_path}' tested successfully.")
            return True
        else:
            self._log(logging.ERROR, f"Failed to test archive '{archive_path}'. Stdout: {stdout.strip()}, Stderr: {stderr.strip()}")
            return False

    def get_supported_formats(self) -> List[str]:
        """
        Retrieves a list of formats that bz.exe can create/specify.
        This list is based on the -fmt: switch in the bz.exe help output.
        Note: bz.exe can typically *decompress* more formats than it can *create*.
        """
        super().get_supported_formats() # Call super method for availability check
        # Formats listed under -fmt: switch in bz.exe help
        return ["zip", "zipx", "exe", "tar", "tgz", "lzh", "iso", "7z", "gz", "xz"]

if __name__ == "__main__":
    Decompressor.enable_logging(True) # Enable logging for all decompressor instances

    print("--- Instantiating BandizipDecompressor ---")
    bandizip_decompressor = BandizipDecompressor()
    
    # Initial availability check
    print(f"Initial availability: {bandizip_decompressor.is_available()}")

    # Define common paths for bz.exe to try and find it
    bz_exe_paths = [
        shutil.which("bz.exe"), # Try finding in PATH
        shutil.which("bz"),     # Try finding in PATH (Linux/macOS might use 'bz' directly)
        os.path.join(os.environ.get('ProgramFiles', 'C:\\Program Files'), 'Bandizip', 'bz.exe'),
        os.path.join(os.environ.get('ProgramFiles(x86)', 'C:\\Program Files (x86)'), 'Bandizip', 'bz.exe'),
        # Add more common paths if needed for your environment (e.g., specific Linux/macOS paths)
    ]
    
    found_bz_exe_path = None
    for path in bz_exe_paths:
        if path and os.path.exists(path):
            found_bz_exe_path = path
            break

    # --- Test Case 1: Attempt operation if not available ---
    print("\n--- Testing operation if not initially available ---")
    if not bandizip_decompressor.is_available():
        try:
            bandizip_decompressor.decompress("dummy.zip", "output_dir_unavailable")
        except RuntimeError as e:
            print(f"Caught expected error: {e}")
    else:
        print("Decompressor is initially available, skipping unavailable operation test.")

    # --- Test Case 2: Manually set an invalid path ---
    print("\n--- Trying to set an invalid path ---")
    invalid_path = "/path/to/definitely/not/bz.exe"
    if not bandizip_decompressor.set_executable_path(invalid_path):
        print(f"Correctly failed to set path: {invalid_path}")
    print(f"Availability after invalid path: {bandizip_decompressor.is_available()}")

    # --- Test Case 3: Manually set a valid path ---
    print("\n--- Trying to set a valid custom path ---")
    if found_bz_exe_path:
        print(f"Attempting to set custom path: {found_bz_exe_path}")
        if bandizip_decompressor.set_executable_path(found_bz_exe_path):
            print(f"Manually set and verified 'bz.exe' successfully. Availability: {bandizip_decompressor.is_available()}")
        else:
            print(f"Failed to manually set '{found_bz_exe_path}'. Availability: {bandizip_decompressor.is_available()}")
    else:
        print("No valid 'bz.exe' path found for manual testing. Please install Bandizip or set 'found_bz_exe_path' manually.")
        print("Skipping further operations as bz.exe is not available.")
        exit() # Exit if bz.exe is not found, as subsequent tests depend on it.


    # --- Setup for actual operations ---
    test_archive_name = "test_archive_bz.zip"
    test_password_archive_name = "test_password_archive_bz.zip"
    test_output_dir = "bz_output"
    test_password = "mysecretpassword"

    # Clean up previous runs
    if os.path.exists(test_output_dir):
        shutil.rmtree(test_output_dir)
    if os.path.exists(test_archive_name):
        os.remove(test_archive_name)
    if os.path.exists(test_password_archive_name):
        os.remove(test_password_archive_name)

    # Create dummy files for archives
    os.makedirs("temp_files_for_archive", exist_ok=True)
    with open(os.path.join("temp_files_for_archive", "file1.txt"), "w") as f:
        f.write("This is file 1.")
    with open(os.path.join("temp_files_for_archive", "file2.txt"), "w") as f:
        f.write("This is file 2.")
    os.makedirs(os.path.join("temp_files_for_archive", "subdir"), exist_ok=True)
    with open(os.path.join("temp_files_for_archive", "subdir", "nested.txt"), "w") as f:
        f.write("This is a nested file.")

    # Create test_archive_bz.zip using bz.exe
    print(f"\n--- Creating dummy archive: {test_archive_name} ---")
    create_command_zip = [
        bandizip_decompressor.executable_path, "a", test_archive_name,
        os.path.join("temp_files_for_archive", "file1.txt"),
        os.path.join("temp_files_for_archive", "file2.txt"),
        os.path.join("temp_files_for_archive", "subdir", "nested.txt")
    ]
    create_res_zip = subprocess.run(create_command_zip, capture_output=True, text=True)
    if create_res_zip.returncode == 0:
        print(f"Successfully created '{test_archive_name}'.")
    else:
        print(f"Failed to create '{test_archive_name}': {create_res_zip.stderr.strip()}")
        test_archive_name = None # Disable further tests if creation failed

    # Create password-protected archive using bz.exe
    print(f"\n--- Creating password-protected dummy archive: {test_password_archive_name} ---")
    create_command_password_zip = [
        bandizip_decompressor.executable_path, "a", test_password_archive_name,
        os.path.join("temp_files_for_archive", "file1.txt"),
        "-p:" + test_password # Add password switch
    ]
    create_res_password_zip = subprocess.run(create_command_password_zip, capture_output=True, text=True)
    if create_res_password_zip.returncode == 0:
        print(f"Successfully created '{test_password_archive_name}'.")
    else:
        print(f"Failed to create '{test_password_archive_name}': {create_res_password_zip.stderr.strip()}")
        test_password_archive_name = None # Disable further tests if creation failed

    # Cleanup temporary files used for archive creation
    if os.path.exists("temp_files_for_archive"):
        shutil.rmtree("temp_files_for_archive")


    # --- Perform operations if available ---
    if bandizip_decompressor.is_available():
        print("\n--- Decompressor is available. Attempting operations... ---")
        
        if not test_archive_name or not os.path.exists(test_archive_name):
            print(f"\nWarning: '{test_archive_name}' not found. Skipping non-password decompression/listing/testing demo.")
        else:
            print(f"\n--- Testing decompression of '{test_archive_name}' ---")
            current_output_dir = os.path.join(test_output_dir, "decompressed_non_password")
            if bandizip_decompressor.decompress(test_archive_name, current_output_dir):
                print(f"Decompression of '{test_archive_name}' successful.")
                print(f"Contents in '{current_output_dir}': {os.listdir(current_output_dir)}")
            else:
                print(f"Decompression of '{test_archive_name}' failed.")

            print(f"\n--- Testing listing contents of '{test_archive_name}' ---")
            contents = bandizip_decompressor.list_contents(test_archive_name)
            if contents:
                print(f"Contents of '{test_archive_name}':")
                for item in contents:
                    print(f"  Name: {item.get('name')}, Type: {item.get('type')}, Size: {item.get('size')}, Packed: {item.get('packed_size')}")
            else:
                print(f"Failed to list contents of '{test_archive_name}'.")

            print(f"\n--- Testing integrity of '{test_archive_name}' ---")
            if bandizip_decompressor.test_archive(test_archive_name):
                print(f"Integrity test of '{test_archive_name}' successful.")
            else:
                print(f"Integrity test of '{test_archive_name}' failed.")

        if not test_password_archive_name or not os.path.exists(test_password_archive_name):
            print(f"\nWarning: '{test_password_archive_name}' not found. Skipping password-protected archive tests.")
        else:
            print(f"\n--- Testing password-protected archive: {test_password_archive_name} ---")

            # Test Listing with Correct Password
            print(f"\nAttempting to list contents of '{test_password_archive_name}' with correct password...")
            contents_with_pass = bandizip_decompressor.list_contents(test_password_archive_name, password=test_password)
            if contents_with_pass:
                print("Password-protected Archive Contents (with correct password):")
                for item in contents_with_pass:
                    print(f"  - Name: {item.get('name')}, Type: {item.get('type')}, Size: {item.get('size')}")
            else:
                print("Failed to list contents with correct password. (Check bz.exe output if no clear error is logged.)")

            # Test Listing with Incorrect Password
            print(f"\nAttempting to list contents of '{test_password_archive_name}' with INCORRECT password...")
            contents_with_wrong_pass = bandizip_decompressor.list_contents(test_password_archive_name, password="wrongpassword")
            if not contents_with_wrong_pass:
                print("Failed to list contents with incorrect password (Expected behavior).")
            else:
                print("Password-protected Archive Contents (with wrong password) - Unexpectedly succeeded:")
                for item in contents_with_wrong_pass:
                    print(f"  - Name: {item.get('name')}, Type: {item.get('type')}, Size: {item.get('size')}")

            # Test Decompression with Correct Password
            current_output_dir = os.path.join(test_output_dir, "decompressed_password_correct")
            print(f"\nAttempting to decompress '{test_password_archive_name}' with correct password to '{current_output_dir}'...")
            decompressed_pass = bandizip_decompressor.decompress(test_password_archive_name, current_output_dir, password=test_password)
            if decompressed_pass:
                print(f"Password decompression successful. Contents in '{current_output_dir}': {os.listdir(current_output_dir)}")
            else:
                print("Password decompression failed.")

            # Test Decompression with Incorrect Password
            current_output_dir = os.path.join(test_output_dir, "decompressed_password_incorrect")
            print(f"\nAttempting to decompress '{test_password_archive_name}' with INCORRECT password to '{current_output_dir}'...")
            decompressed_wrong_pass = bandizip_decompressor.decompress(test_password_archive_name, current_output_dir, password="wrongpassword")
            if not decompressed_wrong_pass: 
                print("Decompression with incorrect password failed as expected.")
            else:
                print("Decompression with incorrect password unexpectedly succeeded.")

            # Test Archive Integrity with Correct Password
            print(f"\nAttempting to test integrity of '{test_password_archive_name}' with correct password...")
            tested_pass = bandizip_decompressor.test_archive(test_password_archive_name, password=test_password)
            if tested_pass:
                print("Password-protected archive integrity test passed.")
            else:
                print("Password-protected archive integrity test failed.")
            
            # Test Archive Integrity with Incorrect Password
            print(f"\nAttempting to test integrity of '{test_password_archive_name}' with INCORRECT password...")
            tested_wrong_pass = bandizip_decompressor.test_archive(test_password_archive_name, password="wrongpassword")
            if not tested_wrong_pass: 
                print("Integrity test with incorrect password failed as expected.")
            else:
                print("Integrity test with incorrect password unexpectedly succeeded.")

    # --- Cleanup ---
    print("\n--- Cleaning up test artifacts ---")
    if os.path.exists(test_output_dir):
        shutil.rmtree(test_output_dir)
    if os.path.exists(test_archive_name):
        os.remove(test_archive_name)
    if os.path.exists(test_password_archive_name):
        os.remove(test_password_archive_name)
    print("Cleanup complete.")