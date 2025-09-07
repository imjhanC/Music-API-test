from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import yt_dlp
import uvicorn
from typing import List, Dict, Optional
import asyncio
import concurrent.futures
from pydantic import BaseModel
import requests
import json
import time
import random

app = FastAPI(title="Music Streaming API", version="1.0.0")

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

def get_enhanced_ydl_opts():
    """Get enhanced yt-dlp options to avoid bot detection"""
    return {
        'format': 'bestaudio[abr>0]/bestaudio/best',
        'quiet': True,
        'no_warnings': True,
        'ignoreerrors': True,
        'geo_bypass': True,
        'socket_timeout': 30,
        'retries': 3,
        'sleep_interval_requests': 1,
        'sleep_interval_subtitles': 1,
        'extractor_args': {
            'youtube': {
                'player_client': ['android', 'web'],
                'skip': ['dash', 'hls']
            }
        },
        # Add user agent rotation
        'http_headers': {
            'User-Agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ])
        }
    }

def perform_search_sync(query: str) -> List[Dict]:
    """Perform YouTube search using yt-dlp"""
    if not query:
        return []
    
    try:
        print(f"Searching for: {query}")
        
        # Enhanced search options
        search_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'skip_download': True,
            'ignoreerrors': True,
            'geo_bypass': True,
            'noplaylist': True,
            'socket_timeout': 15,
            'retries': 2,
            'format': 'best',
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                }
            }
        }
        
        # Add random delay to avoid rate limiting
        time.sleep(random.uniform(0.5, 1.5))
        
        # Fetch 10 results to filter down to 5 good ones
        fetch_count = 10
        with yt_dlp.YoutubeDL(search_opts) as ydl:
            search_results = ydl.extract_info(
                f"ytsearch{fetch_count}:{query}",
                download=False
            )
        
        print(f"yt-dlp response received")
        
        if not search_results or 'entries' not in search_results:
            print("No entries in search results")
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
            
            # Skip if title and uploader are the same (likely not music)
            if str(title).strip().lower() == str(uploader).strip().lower():
                continue
                
            # Skip if duration is 0 or invalid
            if not duration or duration <= 0:
                continue
            
            # Skip very long videos (likely not songs)
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
            
            # Limit to 5 results
            if len(filtered) >= 5:
                break
        
        print(f"Processed {len(filtered)} results")
        return filtered
        
    except Exception as e:
        print(f"yt-dlp search failed: {str(e)}")
        return []

def get_stream_url_with_fallbacks(video_id: str) -> Dict:
    """Get streaming URL with multiple fallback strategies"""
    youtube_url = f"https://www.youtube.com/watch?v={video_id}"
    
    # Strategy 1: Try with enhanced options and different clients
    strategies = [
        # Android client (most reliable)
        {
            **get_enhanced_ydl_opts(),
            'extractor_args': {
                'youtube': {
                    'player_client': ['android'],
                    'skip': ['dash', 'hls']
                }
            }
        },
        # Web client with age gate bypass
        {
            **get_enhanced_ydl_opts(),
            'extractor_args': {
                'youtube': {
                    'player_client': ['web'],
                    'skip': ['dash', 'hls']
                }
            }
        },
        # iOS client
        {
            **get_enhanced_ydl_opts(),
            'extractor_args': {
                'youtube': {
                    'player_client': ['ios'],
                    'skip': ['dash', 'hls']
                }
            }
        },
        # Embed method
        {
            **get_enhanced_ydl_opts(),
            'extractor_args': {
                'youtube': {
                    'player_client': ['web_embedded'],
                }
            }
        }
    ]
    
    for i, opts in enumerate(strategies):
        try:
            print(f"Trying strategy {i+1} for video {video_id}")
            
            # Add random delay between attempts
            if i > 0:
                time.sleep(random.uniform(1, 3))
            
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                
                if info and info.get('url'):
                    print(f"Strategy {i+1} successful!")
                    return {
                        'stream_url': info['url'],
                        'title': info.get('title', 'Unknown Title'),
                        'duration': info.get('duration', 0),
                        'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
                    }
                    
        except Exception as e:
            error_msg = str(e).lower()
            print(f"Strategy {i+1} failed: {e}")
            
            # If it's a bot detection error, continue to next strategy
            if any(keyword in error_msg for keyword in ['bot', 'sign in', '429', 'captcha']):
                continue
            # If it's a different error, we might want to break early
            elif 'private' in error_msg or 'unavailable' in error_msg:
                break
    
    raise Exception(f"All extraction strategies failed for video {video_id}")

def get_stream_url_sync(video_id: str) -> Dict:
    """Get streaming URL for a specific video"""
    try:
        return get_stream_url_with_fallbacks(video_id)
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error getting stream URL: {error_msg}")
        
        # Provide more specific error messages
        if 'bot' in error_msg.lower() or 'sign in' in error_msg.lower():
            raise HTTPException(
                status_code=429, 
                detail="YouTube blocked the request. Please try again in a few minutes."
            )
        elif 'private' in error_msg.lower():
            raise HTTPException(
                status_code=403, 
                detail="This video is private or unavailable."
            )
        elif 'unavailable' in error_msg.lower():
            raise HTTPException(
                status_code=404, 
                detail="Video not found or has been removed."
            )
        else:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to get stream URL: {str(e)}"
            )

# Thread pool for async operations
executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)  # Reduced to avoid rate limits

@app.get("/")
async def root():
    return {"message": "Music Streaming API is running!"}

@app.get("/search", response_model=List[SearchResult])
async def search_music(q: str = Query(..., description="Search query for music")):
    """Search for music on YouTube"""
    if not q or len(q.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")
    
    try:
        # Run search in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(executor, perform_search_sync, q.strip())
        
        if not results:
            return []
        
        return results
        
    except Exception as e:
        print(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")

@app.get("/stream/{video_id}", response_model=StreamResponse)
async def get_stream(video_id: str):
    """Get streaming URL for a specific video"""
    if not video_id:
        raise HTTPException(status_code=400, detail="Video ID is required")
    
    try:
        # Run stream extraction in thread pool
        loop = asyncio.get_event_loop()
        stream_data = await loop.run_in_executor(executor, get_stream_url_sync, video_id)
        
        return stream_data
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Stream error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get stream")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Music Streaming API"}

# Add a test endpoint to check if yt-dlp is working
@app.get("/test/{video_id}")
async def test_extraction(video_id: str):
    """Test endpoint to debug extraction issues"""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, get_stream_url_sync, video_id)
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    print("Starting Enhanced Music Streaming API...")
    print("API will be available at: http://localhost:8000")
    print("Documentation at: http://localhost:8000/docs")
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )