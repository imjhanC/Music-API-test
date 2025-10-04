import asyncio
import threading

# REQUEST DEDUPLICATION SYSTEM
class RequestDeduplicator:
    def __init__(self):
        self.active_requests = {}
        self.lock = threading.RLock()
    
    async def get_or_execute(self, key: str, coro_func, *args, **kwargs):
        with self.lock:
            if key in self.active_requests:
                # Wait for existing request to complete
                print(f"[DEDUP] Waiting for existing request: {key}")
                return await self.active_requests[key]
        
        # Create new request
        print(f"[DEDUP] Creating new request: {key}")
        future = asyncio.create_task(coro_func(*args, **kwargs))
        
        with self.lock:
            self.active_requests[key] = future
        
        try:
            result = await future
            return result
        finally:
            with self.lock:
                self.active_requests.pop(key, None)