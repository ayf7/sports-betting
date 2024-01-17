#!/usr/bin/env python
import time
import colorama
from colorama import Fore

class Logger:
    """
    Logs messages with three.
    """
    def __init__(self, name, enabled=True, time_enabled=True, indent=0):
        self.name = name
        self.start_time = time.time()
        self.enabled = enabled
        self.time_enabled = time_enabled
        self.indent=indent
    
    def format(self, text, color=None, bold=False):
        formatted_text = ""
        if color:
            formatted_text += color
        if bold:
            formatted_text += colorama.Style.BRIGHT
        formatted_text += text + colorama.Style.RESET_ALL
        return formatted_text
    

    def prefix(self, text:str, color:colorama.Fore) -> None:
        """
        Returns a prefix of the form [TEXT].
        """
        prefix = self.format(text, color=color, bold=True)
        left_bracket = self.format("[", bold=True)
        right_bracket = self.format("]", bold=True)
        prefix = f"{left_bracket}{prefix}{right_bracket}"
        return prefix
    
    def info(self, msg:str=""):
        """
        Constructs the INFO prefix and returns it.
        """
        if self.enabled:
            prefix = self.prefix("INFO", color=colorama.Fore.GREEN)
            self._print_msg(prefix, msg)
    
    def warn(self, msg:str=""):
        """
        Constructs the WARN prefix and returns it.
        """
        if self.enabled:
            prefix = self.prefix("WARN", color=colorama.Fore.YELLOW)
            self._print_msg(prefix, msg)
    
    def fail(self, msg:str=""):
        """
        Constructs the FAIL prefix and returns it.
        """
        if self.enabled:
            prefix = self.prefix("FAIL", color=colorama.Fore.RED)
            self._print_msg(prefix, msg)
    
    def _print_msg(self, prefix, msg):
        """
        Prints the message after INFO/WARN/FAIL has been called. Should not be
        called directly.
        """
        # construct time stamp
        current_time = time.time()
        elapsed_time_seconds = current_time - self.start_time
        elapsed_minutes = int(elapsed_time_seconds // 60)
        elapsed_seconds = int(elapsed_time_seconds % 60)
        elapsed_minutes = f"{elapsed_minutes:02d}"
        elapsed_seconds = f"{elapsed_seconds:02d}"
        time_stamp = f" {elapsed_minutes}m {elapsed_seconds}s | "

        # Construct name
        name = self.format(self.name, bold=True)
        repeat = 2 if len(self.name) >= 8 else 3
        tabs = "\t"*repeat
        indent = "  "*self.indent
        print(f"{indent}{time_stamp}{prefix} {name}{tabs}{msg}")
        pass

if __name__ == "__main__":
    logger = Logger("DataHandler")
    logger.info("imported the data handler")
    time.sleep(3)
    logger.fail("failed to create this thing")
    time.sleep(2)
    logger.warn("value not found")
