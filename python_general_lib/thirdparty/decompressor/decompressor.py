import os
import re
import subprocess
import shutil
import logging # Import the logging module
from abc import ABC, abstractmethod
from typing import List, Dict, Optional

class Decompressor(ABC):
    """
    Abstract base class for decompressors, defining common operations for various archiving software.
    Supports deferred setting and validation of the decompressor executable path.
    Includes configurable logging with an on/off switch.
    """

    # --- Class-level Logging Configuration ---
    _logger = logging.getLogger("DecompressorLogger") # Get a logger instance
    _log_enabled = True # Class-level switch for logging

    # Configure the logger (this can be done once, typically at application startup)
    # For demonstration, we'll add a basic handler if none exist.
    if not _logger.handlers:
        _logger.setLevel(logging.INFO) # Set default logging level
        handler = logging.StreamHandler() # Output logs to console
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        _logger.addHandler(handler)

    @classmethod
    def enable_logging(cls, enable: bool):
        """
        Class method to enable or disable logging for all Decompressor instances.
        :param enable: If True, logging is enabled; if False, it's disabled.
        """
        cls._log_enabled = enable
        # Set the logger level based on the switch
        if enable:
            cls._logger.setLevel(logging.INFO)
            cls._logger.info("Decompressor logging enabled.")
        else:
            cls._logger.info("Decompressor logging disabled.") # Log this message BEFORE disabling
            cls._logger.setLevel(logging.CRITICAL + 1) # Set to a level that effectively disables all normal logging

    def __init__(self):
        """
        Constructor.
        Initializes an internal logger instance and the availability state.
        """
        self._executable_path: Optional[str] = None
        self._is_available: bool = False
        
        # Each instance can have its own reference to the shared class logger
        self.logger = Decompressor._logger 

    def _log(self, level: int, message: str, *args, **kwargs):
        """
        Helper method to log messages, respecting the class-level switch.
        :param level: Logging level (e.g., logging.INFO, logging.ERROR).
        :param message: The log message string.
        """
        if Decompressor._log_enabled:
            self.logger.log(level, message, *args, **kwargs)

    def set_executable_path(self, executable_path: str) -> bool:
        """
        Sets and validates the path to the decompressor software's executable file.
        The decompressor is marked as available only if the path is valid and verified.
        :param executable_path: The path to the decompressor software's executable.
        :return: True if the path is valid and the decompressor is marked as available, False otherwise.
        """
        if not os.path.exists(executable_path):
            self._log(logging.ERROR, f"Specified decompressor executable not found: {executable_path}")
            self._is_available = False
            return False
        
        if not self._verify_executable(executable_path):
            self._log(logging.WARNING, f"Executable '{executable_path}' failed verification, it might not be the expected decompressor.")
            self._is_available = False
            return False

        self._executable_path = executable_path
        self._is_available = True
        self._log(logging.INFO, f"Successfully set decompressor executable path to: {executable_path}")
        return True

    def is_available(self) -> bool:
        """
        Checks if the current decompressor instance is available for operations.
        It's available only after a valid executable path has been successfully set.
        :return: True if the decompressor is available, False otherwise.
        """
        return self._is_available

    def _check_availability(self):
        """
        Internal method: Checks the decompressor's availability before performing any operation.
        Raises a RuntimeError if the decompressor is not available.
        """
        if not self._is_available or self._executable_path is None:
            self._log(logging.CRITICAL, "Decompressor is not properly initialized or is unavailable. Please call set_executable_path with a valid path first.")
            raise RuntimeError("Decompressor is not properly initialized or is unavailable. Please call set_executable_path with a valid path first.")

    @abstractmethod
    def _verify_executable(self, executable_path: str) -> bool:
        """
        Abstract private method: Verifies if the executable at the given path meets the requirements
        for this specific decompressor implementation.
        Subclasses must implement this for concrete software identification.
        :param executable_path: The path to the executable file to verify.
        :return: True if the verification passes, False otherwise.
        """
        pass

    @property
    def executable_path(self) -> str:
        """
        Gets the currently set executable file path.
        Should be checked for availability using is_available() before access.
        """
        self._check_availability()
        return self._executable_path

    @abstractmethod
    def decompress(self, archive_path: str, output_path: str, password: Optional[str] = None) -> bool:
        """
        Abstract decompression operation.
        Availability of the decompressor will be checked before execution.
        """
        self._check_availability()
        pass

    @abstractmethod
    def list_contents(self, archive_path: str, password: Optional[str] = None) -> Optional[List[Dict[str, str]]]:
        """
        Abstract listing operation.
        Availability of the decompressor will be checked before execution.
        """
        self._check_availability()
        pass

    @abstractmethod
    def test_archive(self, archive_path: str, password: Optional[str] = None) -> bool:
        """
        Abstract archive testing operation.
        Availability of the decompressor will be checked before execution.
        """
        self._check_availability()
        pass

    @abstractmethod
    def get_supported_formats(self) -> List[str]:
        """
        Abstract method.
        Retrieves a list of supported compression formats for the current decompressor software.
        Availability of the decompressor will be checked before execution.
        """
        self._check_availability()
        pass

    def _execute_command(self, command: List[str]) -> tuple[int, str, str]:
        """
        Private helper method: Executes a command-line instruction.
        Availability of the decompressor will be checked before execution.
        """
        self._check_availability()
        try:
            self._log(logging.DEBUG, f"Executing command: {' '.join(command)}")
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            if process.returncode != 0:
                self._log(logging.WARNING, f"Command exited with non-zero code {process.returncode}: {' '.join(command)}")
                self._log(logging.DEBUG, f"STDOUT: {process.stdout.strip()}")
                self._log(logging.DEBUG, f"STDERR: {process.stderr.strip()}")
            return process.returncode, process.stdout, process.stderr
        except FileNotFoundError:
            self._log(logging.ERROR, f"Executable or command not found: {command[0]} (Check if set_executable_path was correct)")
            return -1, "", "Executable or command not found."
        except Exception as e:
            self._log(logging.ERROR, f"An error occurred while executing the command '{' '.join(command)}': {e}")
            return -1, "", str(e)