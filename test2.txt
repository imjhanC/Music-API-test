from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import uvicorn
from typing import List, Dict
import asyncio
import concurrent.futures
from pydantic import BaseModel
import random
import time
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Music Streaming API", version="2.0.0")

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

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0'
]

def format_duration_fast(seconds):
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

def perform_search_sync(query: str) -> List[Dict]:
    if not query:
        return []
    
    try:
        logger.info(f"Searching for: {query}")
        
        search_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'skip_download': True,
            'ignoreerrors': True,
            'geo_bypass': True,
            'noplaylist': True,
            'socket_timeout': 15,
            'retries': 3,
            'user_agent': random.choice(USER_AGENTS),
            'sleep_interval': 1,
            'max_sleep_interval': 3,
            'extractor_retries': 2,
        }
        
        # Random delay to avoid detection
        time.sleep(random.uniform(0.5, 2.0))
        
        fetch_count = 15
        with yt_dlp.YoutubeDL(search_opts) as ydl:
            search_results = ydl.extract_info(
                f"ytsearch{fetch_count}:{query}",
                download=False
            )
        
        if not search_results or 'entries' not in search_results:
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
            
            if str(title).strip().lower() == str(uploader).strip().lower():
                continue
                
            if not duration or duration <= 0:
                continue
            
            if duration > 600:  # 10 minutes
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
            
            if len(filtered) >= 8:  # Get more results for better success rate
                break
        
        logger.info(f"Processed {len(filtered)} results")
        return filtered
        
    except Exception as e:
        logger.error(f"Search failed: {str(e)}")
        return []

def get_stream_url_sync(video_id: str) -> Dict:
    """Multiple strategies for stream extraction"""
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"
    logger.info(f"Attempting to extract stream for: {video_id}")
    
    # Strategy 1: Standard extraction with enhanced options
    def try_standard_extraction():
        opts = {
            'format': 'bestaudio[abr>0]/bestaudio/best[height<=480]',
            'quiet': True,
            'no_warnings': True,
            'user_agent': random.choice(USER_AGENTS),
            'socket_timeout': 20,
            'retries': 3,
            'extractor_retries': 2,
            'sleep_interval': random.uniform(1, 3),
            'max_sleep_interval': 5,
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                    'skip': ['hls', 'dash'],
                }
            }
        }
        
        time.sleep(random.uniform(1.0, 3.0))
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(youtube_url, download=False)
    
    # Strategy 2: Mobile client simulation
    def try_mobile_extraction():
        opts = {
            'format': 'bestaudio[abr>0]/best[height<=360]',
            'quiet': True,
            'no_warnings': True,
            'user_agent': 'Mozilla/5.0 (Linux; Android 11; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.210 Mobile Safari/537.36',
            'socket_timeout': 25,
            'extractor_args': {
                'youtube': {
                    'player_client': ['android', 'ios'],
                }
            }
        }
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(youtube_url, download=False)
    
    # Strategy 3: Web client with different options
    def try_web_extraction():
        opts = {
            'format': 'bestaudio/best',
            'quiet': True,
            'no_warnings': True,
            'user_agent': random.choice(USER_AGENTS),
            'socket_timeout': 30,
            'extractor_args': {
                'youtube': {
                    'player_client': ['web'],
                    'player_skip': ['configs'],
                }
            }
        }
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(youtube_url, download=False)
    
    # Try strategies with exponential backoff
    strategies = [
        ("Standard Android", try_standard_extraction),
        ("Mobile simulation", try_mobile_extraction),
        ("Web client", try_web_extraction),
    ]
    
    for attempt, (strategy_name, strategy_func) in enumerate(strategies):
        try:
            logger.info(f"Trying {strategy_name} for video {video_id} (attempt {attempt + 1})")
            
            info = strategy_func()
            
            if info and info.get('url'):
                logger.info(f"Success with {strategy_name}")
                return {
                    'stream_url': info['url'],
                    'title': info.get('title', 'Unknown Title'),
                    'duration': info.get('duration', 0),
                    'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
                }
            else:
                logger.warning(f"No URL found with {strategy_name}")
                
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"{strategy_name} failed: {str(e)}")
            
            # Check for specific error types
            if any(keyword in error_msg for keyword in ['sign in', 'bot', 'confirm']):
                # This is the bot detection error - wait longer before next attempt
                if attempt < len(strategies) - 1:
                    wait_time = (attempt + 1) * 2
                    logger.info(f"Bot detection encountered, waiting {wait_time}s before next attempt")
                    time.sleep(wait_time)
            
            continue
    
    # If all strategies fail, provide detailed error
    raise HTTPException(
        status_code=503,
        detail={
            "error": "extraction_failed",
            "message": "Unable to extract stream URL. This is likely due to YouTube's bot detection on cloud servers.",
            "video_id": video_id,
            "suggestions": [
                "Try again in a few minutes",
                "Try a different song",
                "This issue is common on cloud hosting platforms"
            ]
        }
    )

# Thread pool for async operations
executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)

@app.get("/")
async def root():
    return {
        "message": "Music Streaming API v2.0",
        "status": "online",
        "environment": "cloud" if os.getenv("RENDER") else "local",
        "note": "Some tracks may be unavailable due to platform restrictions"
    }

@app.get("/search", response_model=List[SearchResult])
async def search_music(q: str = Query(..., description="Search query for music")):
    if not q or len(q.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")
    
    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(executor, perform_search_sync, q.strip())
        
        return results
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")

@app.get("/stream/{video_id}")
async def get_stream(video_id: str):
    if not video_id:
        raise HTTPException(status_code=400, detail="Video ID is required")
    
    try:
        loop = asyncio.get_event_loop()
        stream_data = await loop.run_in_executor(executor, get_stream_url_sync, video_id)
        
        return stream_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Stream error: {e}")
        raise HTTPException(
            status_code=503, 
            detail={
                "error": "stream_extraction_failed",
                "message": str(e),
                "video_id": video_id
            }
        )

@app.get("/health")
async def health_check():
    return {
        "status": "healthy", 
        "service": "Music Streaming API v2.0",
        "timestamp": time.time()
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    host = "0.0.0.0"
    
    logger.info(f"Starting Music Streaming API on {host}:{port}")
    
    uvicorn.run(
        "main:app",  # Change this to your file name
        host=host,
        port=port,
        log_level="info"
    )