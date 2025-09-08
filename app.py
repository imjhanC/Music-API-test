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

class VideoStreamResponse(BaseModel):
    video_url: str
    audio_url: Optional[str] = None
    title: str
    duration: int
    thumbnail_url: str
    quality: str

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

def perform_search_sync(query: str, limit: Optional[int] = None) -> List[Dict]:
    """Perform YouTube search using yt-dlp with unlimited results"""
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
        
        # Fetch many more results (50 by default, or unlimited if no limit)
        fetch_count = limit if limit else 50
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
            
            # Skip if duration is 0 or invalid
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
            
            # Only limit if a specific limit is provided
            if limit and len(filtered) >= limit:
                break
        
        print(f"Processed {len(filtered)} results")
        return filtered
        
    except Exception as e:
        print(f"yt-dlp search failed: {str(e)}")
        return []

def get_stream_url_sync(video_id: str) -> Dict:
    """Get streaming URL for audio with fast strategy only"""
    try:
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        
        # Fast strategy only - optimized for speed
        opts = {
            'format': 'bestaudio[abr>0]/bestaudio/best',
            'quiet': True,
            'no_warnings': True,
            'extractor_retries': 1,
            'fragment_retries': 1,
            'socket_timeout': 15,
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
        
        print(f"Extracting audio stream for {video_id}")
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            
            if info and info.get('url'):
                print(f"Successfully extracted audio stream")
                return {
                    'stream_url': info['url'],
                    'title': info.get('title', 'Unknown Title'),
                    'duration': info.get('duration', 0),
                    'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
                }
        
        raise Exception("No audio stream found")
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error getting audio stream URL: {error_msg}")
        
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
            raise HTTPException(status_code=500, detail=f"Failed to get audio stream URL: {error_msg}")

def get_video_stream_url_sync(video_id: str) -> Dict:
    """Get streaming URL for video with audio in highest quality"""
    try:
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        
        # Video extraction strategy - prioritize highest quality
        opts = {
            # Advanced format selection for maximum quality
            'format': (
                'bestvideo[height>=2160][ext=mp4]+bestaudio[ext=m4a]/'  # 4K + audio
                'bestvideo[height>=1080][ext=mp4]+bestaudio[ext=m4a]/'  # 1080p + audio
                'bestvideo[height>=720][ext=mp4]+bestaudio[ext=m4a]/'   # 720p + audio
                'best[ext=mp4]/'  # Best single file mp4
                'bestvideo+bestaudio/'  # Best video + best audio (any format)
                'best'  # Fallback to best available
            ),
            'quiet': True,
            'no_warnings': True,
            'extractor_retries': 2,  # Increased retries for high quality extraction
            'fragment_retries': 2,
            'socket_timeout': 30,  # Increased timeout for larger files
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
        
        print(f"Extracting highest quality video stream for {video_id}")
        
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(youtube_url, download=False)
            
            if info:
                # Check if we have a single URL (combined video+audio) or separate streams
                if info.get('url'):
                    # Single combined stream
                    quality = "Unknown"
                    if info.get('height'):
                        quality = f"{info['height']}p"
                    elif info.get('format_note'):
                        quality = info['format_note']
                    
                    print(f"Video quality: {quality}")
                    return {
                        'video_url': info['url'],
                        'title': info.get('title', 'Unknown Title'),
                        'duration': info.get('duration', 0),
                        'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg",  # Higher quality thumbnail
                        'quality': quality
                    }
                
                # Check for separate video and audio streams
                elif 'requested_formats' in info and info['requested_formats']:
                    video_url = None
                    audio_url = None
                    quality = "Unknown"
                    
                    for fmt in info['requested_formats']:
                        if fmt.get('vcodec') != 'none' and fmt.get('acodec') == 'none':
                            # Video stream
                            video_url = fmt.get('url')
                            if fmt.get('height'):
                                quality = f"{fmt['height']}p"
                        elif fmt.get('acodec') != 'none' and fmt.get('vcodec') == 'none':
                            # Audio stream
                            audio_url = fmt.get('url')
                    
                    if video_url:
                        result = {
                            'video_url': video_url,
                            'title': info.get('title', 'Unknown Title'),
                            'duration': info.get('duration', 0),
                            'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg",
                            'quality': quality
                        }
                        if audio_url:
                            result['audio_url'] = audio_url
                        
                        print(f"Video quality: {quality} (separate streams)")
                        return result
                
                # Fallback: try to extract from formats list
                formats = info.get('formats', [])
                if formats:
                    # Find the best video format
                    best_video = None
                    best_audio = None
                    best_height = 0
                    best_audio_quality = 0
                    
                    for fmt in formats:
                        # Check for video formats
                        if fmt.get('vcodec') != 'none' and fmt.get('acodec') == 'none':
                            height = fmt.get('height', 0) or 0
                            if height > best_height and fmt.get('url'):
                                best_height = height
                                best_video = fmt
                        
                        # Check for audio formats
                        elif fmt.get('acodec') != 'none' and fmt.get('vcodec') == 'none':
                            abr = fmt.get('abr', 0) or 0
                            if abr > best_audio_quality and fmt.get('url'):
                                best_audio_quality = abr
                                best_audio = fmt
                    
                    if best_video:
                        quality = f"{best_height}p" if best_height > 0 else "Unknown"
                        result = {
                            'video_url': best_video['url'],
                            'title': info.get('title', 'Unknown Title'),
                            'duration': info.get('duration', 0),
                            'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/maxresdefault.jpg",
                            'quality': quality
                        }
                        if best_audio:
                            result['audio_url'] = best_audio['url']
                        
                        print(f"Video quality: {quality} (manual format selection)")
                        return result
        
        raise Exception("No video stream found")
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error getting video stream URL: {error_msg}")
        
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
            raise HTTPException(status_code=500, detail=f"Failed to get video stream URL: {error_msg}")

# Thread pool for async operations
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

@app.get("/")
async def root():
    return {"message": "Music Streaming API is running!"}

@app.get("/search", response_model=List[SearchResult])
async def search_music(
    q: str = Query(..., description="Search query for music"),
    limit: Optional[int] = Query(None, description="Limit number of results (unlimited by default)")
):
    """Search for music on YouTube with unlimited results by default"""
    if not q or len(q.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query must be at least 2 characters")
    
    try:
        # Run search in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(executor, perform_search_sync, q.strip(), limit)
        
        if not results:
            return []
        
        return results
        
    except Exception as e:
        print(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")

@app.get("/stream/{video_id}", response_model=StreamResponse)
async def get_stream(video_id: str):
    """Get audio streaming URL for a specific video (fast, single strategy)"""
    if not video_id:
        raise HTTPException(status_code=400, detail="Video ID is required")
    
    try:
        # Run stream extraction in thread pool
        loop = asyncio.get_event_loop()
        stream_data = await loop.run_in_executor(executor, get_stream_url_sync, video_id)
        
        return stream_data
        
    except HTTPException:
        # Don't retry HTTP exceptions (they're already handled)
        raise
    except Exception as e:
        print(f"Stream error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get audio stream")

@app.get("/streamvideo/{video_id}", response_model=VideoStreamResponse)
async def get_video_stream(video_id: str):
    """Get highest quality video streaming URL (4K/1080p) with optimized speed"""
    if not video_id:
        raise HTTPException(status_code=400, detail="Video ID is required")
    
    try:
        # Run video stream extraction in thread pool
        loop = asyncio.get_event_loop()
        stream_data = await loop.run_in_executor(executor, get_video_stream_url_sync, video_id)
        
        return stream_data
        
    except HTTPException:
        # Don't retry HTTP exceptions (they're already handled)
        raise
    except Exception as e:
        print(f"Video stream error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get video stream")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Music Streaming API"}

if __name__ == "__main__":
    print("Starting Music Streaming API...")
    print("API will be available at: http://localhost:8000")
    print("Documentation at: http://localhost:8000/docs")
    print("Endpoints:")
    print("  - /search?q=query&limit=10 (limit is optional, unlimited by default)")
    print("  - /stream/VIDEO_ID (audio only, fast)")
    print("  - /streamvideo/VIDEO_ID (video with audio, HIGHEST QUALITY)")
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

## Usage Examples:
## Search unlimited: /search?q=aespa
## Search limited: /search?q=aespa&limit=10
## Audio stream: /stream/5oQVTnq-UKk
## High Quality Video stream: /streamvideo/5oQVTnq-UKk

## To start with ngrok:
## ngrok http --domain=instinctually-monosodium-shawnda.ngrok-free.app 8000
## https://instinctually-monosodium-shawnda.ngrok-free.app/