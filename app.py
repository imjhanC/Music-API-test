from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import yt_dlp
import uvicorn
from typing import List, Dict, Optional
import asyncio
import concurrent.futures
from pydantic import BaseModel
import random
import time

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

def perform_search_sync(query: str) -> List[Dict]:
    """Perform YouTube search using yt-dlp"""
    if not query:
        return []
    
    try:
        print(f"Searching for: {query}")
        
        # Streamlined yt-dlp options for speed
        search_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
            'skip_download': True,
            'ignoreerrors': True,
            'geo_bypass': True,
            'noplaylist': True,
            'socket_timeout': 8,
            'retries': 1,
            'format': 'best',
            # Add anti-detection headers
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

def get_stream_url_sync(video_id: str) -> Dict:
    """Get streaming URL for a specific video with enhanced bot detection avoidance"""
    try:
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        
        # List of different user agents to rotate through
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        # Different extraction strategies
        strategies = [
            # Strategy 1: Basic with random user agent
            {
                'format': 'bestaudio[abr>0]/bestaudio/best',
                'quiet': True,
                'no_warnings': True,
                'extractor_retries': 3,
                'fragment_retries': 3,
                'socket_timeout': 30,
                'http_headers': {
                    'User-Agent': random.choice(user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-us,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
            },
            
            # Strategy 2: Mobile user agent
            {
                'format': 'bestaudio[abr>0]/bestaudio/best',
                'quiet': True,
                'no_warnings': True,
                'extractor_retries': 2,
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                }
            },
            
            # Strategy 3: Different approach with geo bypass
            {
                'format': 'bestaudio[abr>0]/bestaudio/best',
                'quiet': True,
                'no_warnings': True,
                'geo_bypass': True,
                'geo_bypass_country': 'US',
                'http_headers': {
                    'User-Agent': random.choice(user_agents),
                    'Referer': 'https://www.youtube.com/',
                }
            },
            
            # Strategy 4: Minimalist approach
            {
                'format': 'bestaudio/best',
                'quiet': True,
                'http_headers': {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                }
            }
        ]
        
        last_error = None
        
        for i, opts in enumerate(strategies):
            try:
                print(f"Trying extraction strategy {i+1}")
                
                # Add small random delay to avoid rate limiting
                if i > 0:
                    time.sleep(random.uniform(0.5, 2.0))
                
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(youtube_url, download=False)
                    
                    if info and info.get('url'):
                        print(f"Successfully extracted using strategy {i+1}")
                        return {
                            'stream_url': info['url'],
                            'title': info.get('title', 'Unknown Title'),
                            'duration': info.get('duration', 0),
                            'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
                        }
                
            except Exception as e:
                error_msg = str(e).lower()
                print(f"Strategy {i+1} failed: {str(e)}")
                last_error = e
                
                # If it's a bot detection error, continue to next strategy
                if any(keyword in error_msg for keyword in ['bot', 'sign in', 'confirm', '429', 'too many requests']):
                    print("Bot detection triggered, trying next strategy...")
                    continue
                    
                # If it's a different error, continue but note it
                continue
        
        # If all strategies failed, raise the last error
        if last_error:
            raise last_error
        else:
            raise Exception("All extraction strategies failed")
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error getting stream URL: {error_msg}")
        
        # Provide more specific error messages
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
            raise HTTPException(status_code=500, detail=f"Failed to get stream URL: {error_msg}")

# Thread pool for async operations
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

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
    """Get streaming URL for a specific video with retry logic"""
    if not video_id:
        raise HTTPException(status_code=400, detail="Video ID is required")
    
    max_retries = 2
    for attempt in range(max_retries):
        try:
            # Add random delay between attempts to avoid rate limiting
            if attempt > 0:
                await asyncio.sleep(random.uniform(1, 3))
            
            # Run stream extraction in thread pool
            loop = asyncio.get_event_loop()
            stream_data = await loop.run_in_executor(executor, get_stream_url_sync, video_id)
            
            return stream_data
            
        except HTTPException:
            # Don't retry HTTP exceptions (they're already handled)
            raise
        except Exception as e:
            print(f"Stream error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise HTTPException(status_code=500, detail="Failed to get stream after multiple attempts")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Music Streaming API"}

if __name__ == "__main__":
    print("Starting Music Streaming API...")
    print("API will be available at: http://localhost:8000")
    print("Documentation at: http://localhost:8000/docs")
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )