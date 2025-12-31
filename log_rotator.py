import os
import shutil


class RotatingFileWriter:
    """
    A file-like object that automatically rotates log files when they exceed a size threshold.
    Compatible with subprocess.Popen stdout/stderr redirection.
    """
    
    def __init__(self, filename, max_bytes=3145728, backup_count=4):
        """
        Initialize the rotating file writer.
        
        Args:
            filename: Path to the log file
            max_bytes: Maximum file size in bytes before rotation (default: 3MB)
            backup_count: Number of backup files to keep (default: 4)
        """
        self.filename = filename
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self._file = None
        self._open()
    
    def _open(self):
        """Open the log file in append mode."""
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        self._file = open(self.filename, 'a', buffering=1)  # Line buffered
    
    def _should_rotate(self):
        """Check if the file should be rotated based on size."""
        try:
            return os.path.getsize(self.filename) >= self.max_bytes
        except OSError:
            return False
    
    def _do_rotate(self):
        """Perform the log rotation."""
        if self._file:
            self._file.close()
        
        # Rotate existing backup files
        for i in range(self.backup_count - 1, 0, -1):
            src = f"{self.filename}.{i}"
            dst = f"{self.filename}.{i + 1}"
            if os.path.exists(src):
                if os.path.exists(dst):
                    os.remove(dst)
                shutil.move(src, dst)
        
        # Move current file to .1
        if os.path.exists(self.filename):
            dst = f"{self.filename}.1"
            if os.path.exists(dst):
                os.remove(dst)
            shutil.move(self.filename, dst)
        
        # Open new file
        self._open()
    
    def write(self, data):
        """Write data to the file, rotating if necessary."""
        if self._should_rotate():
            self._do_rotate()
        
        if self._file:
            return self._file.write(data)
        return 0
    
    def flush(self):
        """Flush the file buffer."""
        if self._file:
            self._file.flush()
    
    def fileno(self):
        """Return the file descriptor (required for subprocess)."""
        if self._file:
            return self._file.fileno()
        return -1
    
    def close(self):
        """Close the file."""
        if self._file:
            self._file.close()
            self._file = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
