from datetime import datetime

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
ENDC = "\033[0m"

def log(message):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - {message}")