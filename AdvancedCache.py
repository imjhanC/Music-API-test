import threading
from datetime import datetime, timedelta
from typing import Optional, Dict

class AdvancedCache: ## Advanced Caching System  
    def __init__(self, max_size: int = 1000, ttl_minutes: int = 30):
        self.cache = {}
        self.access_times = {}
        self.max_size = max_size
        self.ttl = timedelta(minutes=ttl_minutes)
        self.lock = threading.RLock()
        
    def get(self, key: str) -> Optional[Dict]:
        with self.lock:
            if key in self.cache:
                # Check if expired
                if datetime.now() - self.access_times[key] > self.ttl:
                    del self.cache[key]
                    del self.access_times[key]
                    return None
                
                # Update access time for LRU
                self.access_times[key] = datetime.now()
                return self.cache[key].copy()
            return None
    
    def set(self, key: str, value: Dict):
        with self.lock:
            # Clean up expired entries
            self._cleanup_expired()
            
            # If cache is full, remove oldest entries
            if len(self.cache) >= self.max_size:
                self._evict_lru()
            
            self.cache[key] = value.copy()
            self.access_times[key] = datetime.now()
    
    def _cleanup_expired(self):
        current_time = datetime.now()
        expired_keys = [
            key for key, access_time in self.access_times.items()
            if current_time - access_time > self.ttl
        ]
        for key in expired_keys:
            del self.cache[key]
            del self.access_times[key]
    
    def _evict_lru(self):
        # Remove 20% of oldest entries
        items_to_remove = max(1, len(self.cache) // 5)
        sorted_items = sorted(self.access_times.items(), key=lambda x: x[1])
        for key, _ in sorted_items[:items_to_remove]:
            del self.cache[key]
            del self.access_times[key]
    
    def clear(self):
        with self.lock:
            self.cache.clear()
            self.access_times.clear()
    
    def stats(self):
        with self.lock:
            return {
                "size": len(self.cache),
                "max_size": self.max_size,
                "hit_ratio": getattr(self, '_hits', 0) / max(getattr(self, '_requests', 1), 1)
            }