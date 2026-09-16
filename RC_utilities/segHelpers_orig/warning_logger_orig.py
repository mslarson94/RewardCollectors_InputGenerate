
import os

class WarningLogger:
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.warnings = []

    def log(self, message):
        print(f"⚠️ {message}")
        self.warnings.append(message)

    def save(self, filename="processing_warnings.txt"):
        os.makedirs(self.output_dir, exist_ok=True)
        log_path = os.path.join(self.output_dir, filename)
        with open(log_path, "w") as f:
            for line in self.warnings:
                f.write(line + "\n")
        print(f"⚠️ Warnings saved to: {log_path}")
