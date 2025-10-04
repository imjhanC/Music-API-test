import threading
import concurrent.futures
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List

# ENHANCED RATE LIMITING AND LOAD BALANCING
class LoadBalancer:
    def __init__(self):
        self.request_counts = defaultdict(int)
        self.last_reset = datetime.now()
        self.lock = threading.RLock()
    
    def get_least_loaded_executor(self, executors: List[concurrent.futures.ThreadPoolExecutor]) -> concurrent.futures.ThreadPoolExecutor:
        with self.lock:
            # Reset counters every minute
            if datetime.now() - self.last_reset > timedelta(minutes=1):
                self.request_counts.clear()
                self.last_reset = datetime.now()
            
            # Find executor with least active requests
            best_executor = min(executors, key=lambda e: len(e._threads) if e._threads else 0)
            executor_id = id(best_executor)
            self.request_counts[executor_id] += 1
            
            return best_executor