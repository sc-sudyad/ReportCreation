import os
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import zipfile


class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, base_filename, when='m', interval=1, backupCount=0, encoding='utf-8'):
        self.base_filename = base_filename
        super().__init__(filename=self.get_log_filename(), when=when, interval=interval, backupCount=backupCount, encoding=encoding)

    def get_log_filename(self):
        current_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        return f"{self.base_filename}-{current_date}.log"

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        current_log_filename = self.baseFilename
        zip_file = current_log_filename.replace('.log', '.log.zip')

        try:
            # Compress the log file
            with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(current_log_filename, os.path.basename(current_log_filename))

            # Remove the original log file after compressing
            if os.path.exists(current_log_filename):
                os.remove(current_log_filename)
                print(f"Deleted original log file {current_log_filename}")

        except Exception as e:
            print(f"Error compressing the log file: {e}")

        super().doRollover()

        self.baseFilename = self.get_log_filename()
        self.stream = self._open()

        self.cleanup_old_logs()

    def cleanup_old_logs(self):
        log_dir = os.path.dirname(self.base_filename)
        log_files = [f for f in os.listdir(log_dir) if f.endswith('.log')]

        # Get a list of log files and sort them by modification time
        log_files.sort(key=lambda f: os.path.getmtime(os.path.join(log_dir, f)))

        # Only keep the most recent log file
        for log_file in log_files[:-1]:
            log_file_path = os.path.join(log_dir, log_file)
            try:
                os.remove(log_file_path)
                print(f"Deleted old log file {log_file_path}")
            except Exception as e:
                print(f"Error deleting old log file {log_file_path}: {e}")


def setup_logging():
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    base_log_file = os.path.join(log_dir, 'app')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = CustomTimedRotatingFileHandler(base_filename=base_log_file, when='m', interval=1, backupCount=0)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear existing handlers to avoid duplicates
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
