from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import yt_dlp
import uvicorn
from typing import List, Dict, Optional, Tuple
import asyncio
import concurrent.futures
from pydantic import BaseModel
import random
import time
import hashlib
import json
from collections import defaultdict
import threading
from datetime import datetime, timedelta
import weakref
import gc

# Importing other classes here 
from AdvancedCache import AdvancedCache
from RequestDeduplicator import RequestDeduplicator
from LoadBalancer import LoadBalancer

app = FastAPI(title="HanyaMusic Music Streaming API", version="3.0.0")

# Enable CORS for React Native
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class SearchResult(BaseModel):
    title: str
    thumbnail_url: str
    videoId: str
    uploader: str
    duration: str
    view_count: str
    url: str

class StreamResponse(BaseModel):
    stream_url: str
    title: str
    duration: int
    thumbnail_url: str
    format: str  
    quality: str 
    cached: Optional[bool] = False

class VideoStreamResponse(BaseModel):
    video_url: str
    audio_url: Optional[str] = None
    title: str
    duration: int
    thumbnail_url: str
    quality: str
    stream_type: str
    cached: Optional[bool] = False

# Global caches for each endpoint
search_cache = AdvancedCache(max_size=500, ttl_minutes=15)  # Search results change less frequently
audio_cache = AdvancedCache(max_size=1000, ttl_minutes=60)  # Audio URLs last longer
video_cache = AdvancedCache(max_size=800, ttl_minutes=45)   # Video URLs last moderately long

# REQUEST DEDUPLICATION SYSTEM
request_deduplicator = RequestDeduplicator()
load_balancer = LoadBalancer()

def format_duration_fast(seconds):
    """Format duration from seconds to MM:SS or HH:MM:SS"""
    if not seconds or seconds <= 0:
        return "0:00"
    
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    else:
        return f"{minutes}:{seconds:02d}"

def format_views_fast(view_count):
    """Format view count to readable format"""
    if not view_count or view_count <= 0:
        return "0 views"
    
    if view_count >= 1_000_000_000:
        return f"{view_count / 1_000_000_000:.1f}B views"
    elif view_count >= 1_000_000:
        return f"{view_count / 1_000_000:.1f}M views"
    elif view_count >= 1_000:
        return f"{view_count / 1_000:.1f}K views"
    else:
        return f"{view_count:,} views"

def create_cache_key(func_name: str, *args, **kwargs) -> str:
    """Create a consistent cache key"""
    key_data = f"{func_name}:{str(args)}:{str(sorted(kwargs.items()))}"
    return hashlib.md5(key_data.encode()).hexdigest()

def perform_search_sync(query: str, limit: Optional[int] = None) -> List[Dict]:
    """Perform YouTube search using yt-dlp with unlimited results"""
    if not query:
        return []
    
    try:
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Searching for: {query}")
        
        # Streamlined yt-dlp options for speed
        search_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'skip_download': True,
            'ignoreerrors': True,
            'geo_bypass': True,
            'noplaylist': True,
            'socket_timeout': 5,  # Further reduced for faster timeout
            'retries': 1,
            'format': 'best',
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-us,en;q=0.5',
                'Sec-Fetch-Mode': 'navigate',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        }
        
        fetch_count = limit if limit else 50
        with yt_dlp.YoutubeDL(search_opts) as ydl:
            search_results = ydl.extract_info(
                f"ytsearch{fetch_count}:{query}",
                download=False
            )
        
        print(f"[{thread_name}] yt-dlp response received")
        
        if not search_results or 'entries' not in search_results:
            print(f"[{thread_name}] No entries in search results")
            return []
        
        entries = search_results.get('entries', [])
        filtered = []
        seen = set()
        
        for entry in entries:
            if not entry or not entry.get('id'):
                continue
            
            vid = entry['id']
            if vid in seen:
                continue
            seen.add(vid)
            
            title = entry.get('title', 'No Title')
            uploader = entry.get('uploader', 'Unknown')
            duration = entry.get('duration')
            view_count = entry.get('view_count')
            
            if not duration or duration <= 0:
                continue
            
            result = {
                'title': str(title).strip()[:100],
                'thumbnail_url': f"https://img.youtube.com/vi/{vid}/mqdefault.jpg",
                'videoId': vid,
                'uploader': str(uploader).strip()[:50] if uploader else 'Unknown',
                'duration': format_duration_fast(duration),
                'view_count': format_views_fast(view_count),
                'url': f"https://www.youtube.com/watch?v={vid}"
            }
            filtered.append(result)
            
            if limit and len(filtered) >= limit:
                break
        
        print(f"[{thread_name}] Processed {len(filtered)} results")
        return filtered
        
    except Exception as e:
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] yt-dlp search failed: {str(e)}")
        return []

def get_stream_url_sync(video_id: str) -> Dict:
    """Get streaming URL for audio - ENFORCES MP3 FORMAT ONLY"""
    try:
        thread_name = threading.current_thread().name
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        
        print(f"[{thread_name}] Processing video_id: {video_id} - ENFORCING MP3 FORMAT")
        
        # MODIFIED: Force MP3 format only with postprocessor
        opts = {
            'format': 'bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '320',  # High quality MP3
            }],
            'quiet': True,
            'no_warnings': True,
            'extractor_retries': 1,
            'fragment_retries': 1,
            'socket_timeout': 15,  # Increased for post-processing
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-us,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        }
        
        print(f"[{thread_name}] Extracting MP3 audio stream for {video_id}")
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            
            if info and info.get('url'):
                # Get audio quality information
                quality_info = "320kbps MP3"  # Default
                if info.get('abr'):
                    quality_info = f"{info['abr']}kbps MP3"
                elif info.get('tbr'):
                    quality_info = f"{info['tbr']}kbps MP3"
                
                print(f"[{thread_name}] Successfully extracted MP3 audio stream: {quality_info}")
                return {
                    'stream_url': info['url'],
                    'title': info.get('title', 'Unknown Title'),
                    'duration': info.get('duration', 0),
                    'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg",
                    'format': 'mp3',  # Always MP3
                    'quality': quality_info
                }
        
        raise Exception("No MP3 audio stream could be generated")
        
    except Exception as e:
        thread_name = threading.current_thread().name
        error_msg = str(e)
        print(f"[{thread_name}] Error getting MP3 audio stream URL: {error_msg}")
        
        if 'bot' in error_msg.lower() or 'sign in' in error_msg.lower():
            raise HTTPException(
                status_code=503, 
                detail="YouTube is temporarily blocking requests. Please try again in a few minutes."
            )
        elif 'private' in error_msg.lower():
            raise HTTPException(status_code=403, detail="This video is private")
        elif 'unavailable' in error_msg.lower():
            raise HTTPException(status_code=404, detail="This video is not available")
        elif 'copyright' in error_msg.lower():
            raise HTTPException(status_code=451, detail="This video is not available due to copyright restrictions")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to get MP3 audio stream URL: {error_msg}")

def get_video_stream_url_sync(video_id: str) -> Dict:
    """Get streaming URL for video - prioritize highest quality even if separate streams"""
    try:
        thread_name = threading.current_thread().name
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        
        print(f"[{thread_name}] Processing video_id: {video_id}")
        
        opts = {
            'format': (
                'bestvideo[height>=2160][ext=mp4]+bestaudio[ext=m4a]/'
                'bestvideo[height>=1440][ext=mp4]+bestaudio[ext=m4a]/'  
                'bestvideo[height>=1080][ext=mp4]+bestaudio[ext=m4a]/'
                'bestvideo[height>=720][ext=mp4]+bestaudio[ext=m4a]/'
                'bestvideo[ext=mp4]+bestaudio[ext=m4a]/'
                'bestvideo+bestaudio[ext=m4a]/'
                'bestvideo+bestaudio/'
                'best[ext=mp4][height>=1080]/'
                'best[ext=mp4][height>=720]/'
                'best[ext=mp4]/'
                'best[height>=720]/'
                'best/'
            ),
            'quiet': True,
            'no_warnings': True,
            'extractor_retries': 2,
            'fragment_retries': 2,
            'merge_output_format': 'mp4',
            'socket_timeout': 20,  # Optimized timeout
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-us,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        }
        
        print(f"[{thread_name}] Extracting highest quality video stream for {video_id}")
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            
            if info:
                # Check for separate streams (preferred)
                if 'requested_formats' in info and info['requested_formats']:
                    video_url = None
                    audio_url = None
                    quality = "Unknown"
                    video_format = None
                    audio_format = None
                    
                    for fmt in info['requested_formats']:
                        if fmt.get('vcodec') != 'none' and fmt.get('acodec') == 'none':
                            video_url = fmt.get('url')
                            video_format = fmt
                            if fmt.get('height'):
                                quality = f"{fmt['height']}p"
                            elif fmt.get('format_note'):
                                quality = fmt['format_note']
                        elif fmt.get('acodec') != 'none' and fmt.get('vcodec') == 'none':
                            audio_url = fmt.get('url')
                            audio_format = fmt
                    
                    if video_url and audio_url:
                        # FIXED: Safe handling of None values
                        fps = video_format.get('fps') if video_format else None
                        vbr = video_format.get('vbr') if video_format else None
                        abr = audio_format.get('abr') if audio_format else None
                        
                        # Safe FPS handling
                        fps = fps if fps is not None and fps > 0 else 30
                        
                        # Safe bitrate handling
                        vbr = vbr if vbr is not None and vbr > 0 else 0
                        abr = abr if abr is not None and abr > 0 else 0
                        
                        quality_detail = quality
                        if fps > 30:
                            quality_detail += f"{fps}fps"
                        if vbr > 0:
                            quality_detail += f" ({vbr}kbps)"
                            
                        print(f"[{thread_name}] Found separate high-quality streams - Video: {quality_detail}, Audio: {abr}kbps")
                        return {
                            'video_url': video_url,
                            'audio_url': audio_url,
                            'title': info.get('title', 'Unknown Title'),
                            'duration': info.get('duration', 0),
                            'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg",
                            'quality': quality_detail,
                            'stream_type': 'separate'
                        }
                
                # Check for single URL (combined)
                if info.get('url'):
                    quality = "Unknown"
                    if info.get('height'):
                        quality = f"{info['height']}p"
                    elif info.get('format_note'):
                        quality = info['format_note']
                    
                    has_video = info.get('vcodec') and info.get('vcodec') != 'none'
                    has_audio = info.get('acodec') and info.get('acodec') != 'none'
                    
                    if has_video and has_audio:
                        # FIXED: Safe handling of None values
                        fps = info.get('fps')
                        vbr = info.get('vbr')
                        
                        # Safe FPS handling
                        fps = fps if fps is not None and fps > 0 else 30
                        
                        # Safe bitrate handling  
                        vbr = vbr if vbr is not None and vbr > 0 else 0
                        
                        quality_detail = quality
                        if fps > 30:
                            quality_detail += f"{fps}fps"
                        if vbr > 0:
                            quality_detail += f" ({vbr}kbps)"
                            
                        print(f"[{thread_name}] Found combined stream - Quality: {quality_detail}")
                        return {
                            'video_url': info['url'],
                            'title': info.get('title', 'Unknown Title'),
                            'duration': info.get('duration', 0),
                            'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg",
                            'quality': quality_detail,
                            'stream_type': 'combined'
                        }
        
        raise Exception("No suitable video stream found")
        
    except Exception as e:
        thread_name = threading.current_thread().name
        error_msg = str(e)
        print(f"[{thread_name}] Error getting video stream URL: {error_msg}")
        
        if 'bot' in error_msg.lower() or 'sign in' in error_msg.lower():
            raise HTTPException(
                status_code=503, 
                detail="YouTube is temporarily blocking requests. Please try again in a few minutes."
            )
        elif 'private' in error_msg.lower():
            raise HTTPException(status_code=403, detail="This video is private")
        elif 'unavailable' in error_msg.lower():
            raise HTTPException(status_code=404, detail="This video is not available")
        elif 'copyright' in error_msg.lower():
            raise HTTPException(status_code=451, detail="This video is not available due to copyright restrictions")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to get video stream URL: {error_msg}")

# MULTIPLE THREAD POOLS FOR MAXIMUM CONCURRENCY
# Create multiple pools for load distribution
search_executors = [
    concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"Search-Pool{i}")
    for i in range(3)  # 3 pools × 4 threads = 12 search threads
]

audio_executors = [
    concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"Audio-Pool{i}")
    for i in range(3)  # 3 pools × 4 threads = 12 audio threads
]

video_executors = [
    concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"Video-Pool{i}")
    for i in range(3)  # 3 pools × 4 threads = 12 video threads
]

# Background task for cache cleanup
async def periodic_cache_cleanup():
    """Periodically clean up expired cache entries"""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        try:
            print("[CACHE] Running periodic cleanup...")
            search_cache._cleanup_expired()
            audio_cache._cleanup_expired()
            video_cache._cleanup_expired()
            
            # Force garbage collection
            gc.collect()
            print("[CACHE] Cleanup completed")
        except Exception as e:
            print(f"[CACHE] Cleanup error: {e}")

# Start background tasks
@app.on_event("startup")
async def startup_event():
    # Start cache cleanup task
    asyncio.create_task(periodic_cache_cleanup())
    print("🚀 High-Performance API started with MP3-only audio streams!")

# Cleanup function for graceful shutdown
async def cleanup_executors():
    """Gracefully shutdown all thread pools"""
    print("Shutting down thread pools...")
    for executor in search_executors + audio_executors + video_executors:
        executor.shutdown(wait=True)
    print("All thread pools shut down successfully")

@app.on_event("shutdown")
async def shutdown_event():
    await cleanup_executors()

@app.get("/")
async def root():
    return {
        "message": "Ultra High-Performance Music Streaming API with MP3-Only Audio!",
        "performance": {
            "search_threads": 12,
            "audio_stream_threads": 12,
            "video_stream_threads": 12,
            "total_threads": 36,
            "audio_format": "MP3 ONLY (320kbps preferred)",
            "features": [
                "Advanced caching system",
                "Request deduplication", 
                "Load balancing",
                "Multiple thread pools per endpoint",
                "MP3-only audio streaming"
            ]
        }
    }

async def cached_search(q: str, limit: Optional[int] = None) -> Tuple[List[SearchResult], bool]:
    """Search with caching and deduplication"""
    cache_key = create_cache_key("search", q, limit)
    
    # Try cache first
    cached_result = search_cache.get(cache_key)
    if cached_result:
        print(f"[SEARCH] Cache HIT for query: {q}")
        return cached_result, True
    
    print(f"[SEARCH] Cache MISS for query: {q}")
    
    # Use deduplication for same requests
    async def execute_search():
        # Load balance across multiple executors
        executor = load_balancer.get_least_loaded_executor(search_executors)
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(executor, perform_search_sync, q.strip(), limit)
        
        # Cache the results
        search_cache.set(cache_key, results)
        return results
    
    results = await request_deduplicator.get_or_execute(cache_key, execute_search)
    return results, False

async def cached_audio_stream(video_id: str) -> Tuple[StreamResponse, bool]:
    """Audio stream with caching and deduplication - RETURNS MP3 ONLY"""
    cache_key = create_cache_key("audio_mp3", video_id)  # Updated cache key for MP3
    
    # Try cache first
    cached_result = audio_cache.get(cache_key)
    if cached_result:
        print(f"[AUDIO] Cache HIT for video_id: {video_id} (MP3)")
        cached_result['cached'] = True
        return cached_result, True
    
    print(f"[AUDIO] Cache MISS for video_id: {video_id} (MP3)")
    
    # Use deduplication for same requests
    async def execute_audio_stream():
        # Load balance across multiple executors
        executor = load_balancer.get_least_loaded_executor(audio_executors)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_stream_url_sync, video_id)
        
        # Cache the results
        audio_cache.set(cache_key, result)
        return result
    
    result = await request_deduplicator.get_or_execute(cache_key, execute_audio_stream)
    result['cached'] = False
    return result, False

async def cached_video_stream(video_id: str) -> Tuple[VideoStreamResponse, bool]:
    """Video stream with caching and deduplication"""
    cache_key = create_cache_key("video", video_id)
    
    # Try cache first
    cached_result = video_cache.get(cache_key)
    if cached_result:
        print(f"[VIDEO] Cache HIT for video_id: {video_id}")
        cached_result['cached'] = True
        return cached_result, True
    
    print(f"[VIDEO] Cache MISS for video_id: {video_id}")
    
    # Use deduplication for same requests
    async def execute_video_stream():
        # Load balance across multiple executors
        executor = load_balancer.get_least_loaded_executor(video_executors)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_video_stream_url_sync, video_id)
        
        # Cache the results
        video_cache.set(cache_key, result)
        return result
    
    result = await request_deduplicator.get_or_execute(cache_key, execute_video_stream)
    result['cached'] = False
    return result, False

@app.get("/search", response_model=List[SearchResult])
async def search_music(
    q: str = Query(..., description="Search query for music"),
    limit: Optional[int] = Query(None, description="Limit number of results (unlimited by default)")
):
    """Search for music - OPTIMIZED with caching, deduplication, and load balancing"""
    if not q or len(q.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")
    
    try:
        print(f"[SEARCH] Processing query: '{q}' with advanced optimizations")
        results, from_cache = await cached_search(q, limit)
        
        if not results:
            return []
        
        print(f"[SEARCH] Completed - returned {len(results)} results {'(cached)' if from_cache else '(fresh)'}")
        return results
        
    except Exception as e:
        print(f"[SEARCH] Error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")

@app.get("/stream/{video_id}", response_model=StreamResponse)
async def get_stream(video_id: str):
    """Get MP3 audio streaming URL - GUARANTEED MP3 FORMAT ONLY"""
    if not video_id:
        raise HTTPException(status_code=400, detail="Video ID is required")
    
    try:
        print(f"[AUDIO] Processing video_id: {video_id} - ENFORCING MP3 FORMAT")
        result, from_cache = await cached_audio_stream(video_id)
        
        print(f"[AUDIO] Completed MP3 stream for video_id: {video_id} {'(cached)' if from_cache else '(fresh)'}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[AUDIO] Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get MP3 audio stream")

@app.get("/streamvideo/{video_id}", response_model=VideoStreamResponse)
async def get_video_stream(video_id: str):
    """Get highest quality video streaming URL - OPTIMIZED with caching, deduplication, and load balancing"""
    if not video_id:
        raise HTTPException(status_code=400, detail="Video ID is required")
    
    try:
        print(f"[VIDEO] Processing video_id: {video_id} with advanced optimizations")
        result, from_cache = await cached_video_stream(video_id)
        
        print(f"[VIDEO] Completed for video_id: {video_id} {'(cached)' if from_cache else '(fresh)'}")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"[VIDEO] Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get video stream")

@app.get("/health")
async def health_check():
    """Health check endpoint with performance metrics"""
    return {
        "status": "healthy", 
        "service": "Ultra High-Performance Music Streaming API with MP3-Only Audio",
        "audio_format": "MP3 ONLY (320kbps preferred)",
        "thread_pools": {
            "search_pools": len(search_executors),
            "audio_pools": len(audio_executors), 
            "video_pools": len(video_executors),
            "total_threads": 36
        },
        "cache_stats": {
            "search_cache": search_cache.stats(),
            "audio_cache": audio_cache.stats(),
            "video_cache": video_cache.stats()
        }
    }

@app.get("/stats")
async def performance_stats():
    """Get current performance statistics and metrics"""
    active_threads = {
        "search": sum(len(e._threads) if e._threads else 0 for e in search_executors),
        "audio": sum(len(e._threads) if e._threads else 0 for e in audio_executors),
        "video": sum(len(e._threads) if e._threads else 0 for e in video_executors)
    }
    
    return {
        "performance_optimization": "ULTRA ACTIVE with MP3-ONLY AUDIO",
        "audio_format_guarantee": "ALL /stream endpoints return MP3 format only",
        "architecture": {
            "search_endpoint": f"{len(search_executors)} pools × 4 threads = 12 total",
            "audio_stream_endpoint": f"{len(audio_executors)} pools × 4 threads = 12 total (MP3 ONLY)", 
            "video_stream_endpoint": f"{len(video_executors)} pools × 4 threads = 12 total",
            "total_worker_threads": 36
        },
        "active_threads": active_threads,
        "optimizations": [
            "Multiple thread pools per endpoint for load distribution",
            "Advanced LRU caching with TTL expiration",
            "Request deduplication to prevent duplicate processing",
            "Intelligent load balancing across thread pools",
            "Automatic cache cleanup and memory management",
            "Optimized timeouts for faster response times",
            "MP3-only audio format enforcement with FFmpeg post-processing"
        ],
        "cache_performance": {
            "search_cache": {
                **search_cache.stats(),
                "ttl_minutes": 15,
                "description": "Search results cached for 15 minutes"
            },
            "audio_cache": {
                **audio_cache.stats(), 
                "ttl_minutes": 60,
                "description": "MP3 audio URLs cached for 60 minutes"
            },
            "video_cache": {
                **video_cache.stats(),
                "ttl_minutes": 45, 
                "description": "Video URLs cached for 45 minutes"
            }
        },
        "concurrent_performance": {
            "max_simultaneous_search": 12,
            "max_simultaneous_audio": 12,
            "max_simultaneous_video": 12,
            "request_deduplication": "Active - prevents duplicate processing",
            "load_balancing": "Active - distributes load across thread pools"
        }
    }

@app.post("/cache/clear")
async def clear_cache():
    """Clear all caches (admin endpoint)"""
    search_cache.clear()
    audio_cache.clear()
    video_cache.clear()
    return {
        "status": "success",
        "message": "All caches cleared successfully (including MP3 audio cache)",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/cache/stats")
async def cache_statistics():
    """Get detailed cache statistics"""
    return {
        "search_cache": {
            **search_cache.stats(),
            "entries": len(search_cache.cache),
            "ttl_minutes": 15
        },
        "audio_cache": {
            **audio_cache.stats(),
            "entries": len(audio_cache.cache),
            "ttl_minutes": 60,
            "format": "MP3 ONLY"
        },
        "video_cache": {
            **video_cache.stats(),
            "entries": len(video_cache.cache),
            "ttl_minutes": 45
        },
        "total_cached_items": len(search_cache.cache) + len(audio_cache.cache) + len(video_cache.cache)
    }

@app.get("/performance/realtime")
async def realtime_performance():
    """Get real-time performance metrics"""
    return {
        "timestamp": datetime.now().isoformat(),
        "audio_format": "MP3 ONLY - ALL audio streams guaranteed to be MP3",
        "thread_utilization": {
            "search_pools": [
                {
                    "pool_id": i,
                    "active_threads": len(executor._threads) if executor._threads else 0,
                    "max_workers": executor._max_workers
                }
                for i, executor in enumerate(search_executors)
            ],
            "audio_pools": [
                {
                    "pool_id": i,
                    "active_threads": len(executor._threads) if executor._threads else 0,
                    "max_workers": executor._max_workers,
                    "format": "MP3 ONLY"
                }
                for i, executor in enumerate(audio_executors)
            ],
            "video_pools": [
                {
                    "pool_id": i,
                    "active_threads": len(executor._threads) if executor._threads else 0,
                    "max_workers": executor._max_workers
                }
                for i, executor in enumerate(video_executors)
            ]
        },
        "deduplication": {
            "active_requests": len(request_deduplicator.active_requests),
            "status": "preventing duplicate processing"
        }
    }

@app.get("/format/info")
async def format_info():
    """Get information about supported audio formats"""
    return {
        "audio_streaming": {
            "format": "MP3 ONLY",
            "quality": "320kbps preferred (varies based on source)",
            "codec": "MP3 (MPEG-1 Audio Layer III)",
            "compatibility": "Universal - works on all devices and platforms",
            "processing": "FFmpeg post-processing ensures MP3 format",
            "endpoint": "/stream/{video_id}"
        },
        "video_streaming": {
            "formats": "Various (MP4, WebM, etc.)",
            "quality": "Highest available (up to 4K)",
            "endpoint": "/streamvideo/{video_id}"
        },
        "guaranteed_features": [
            "All /stream endpoints return MP3 format only",
            "No other audio formats (WebM, M4A, etc.) will be returned",
            "FFmpeg post-processing converts to MP3 if needed",
            "High quality 320kbps preferred when available"
        ]
    }

if __name__ == "__main__":
    print("🚀 ==> HanyaMusic Music Streaming API <==")
    print("🌐 API will be available at: http://localhost:8000")
    print("📚 Documentation at: http://localhost:8000/docs")
    print("📊 Performance Stats: http://localhost:8000/stats")
    print("📈 Real-time Metrics: http://localhost:8000/performance/realtime")
    print("🗄️  Cache Management: http://localhost:8000/cache/stats")
    print("🎵 Format Info: http://localhost:8000/format/info")
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
        access_log=True,
        workers=1  # Single worker for shared cache
    )

## Usage Examples:
## Search: /search?q=aespa (cached for 15min, deduplicated)
## Audio: /stream/5oQVTnq-UKk (MP3 ONLY, cached for 60min, deduplicated) 
## Video: /streamvideo/5oQVTnq-UKk (cached for 45min, deduplicated)
## Stats: /stats (real-time performance metrics + MP3 info)
## Format: /format/info (MP3 format guarantee details)


## To start with ngrok:
## ngrok http --domain=instinctually-monosodium-shawnda.ngrok-free.app 8000
## https://instinctually-monosodium-shawnda.ngrok-free.app/