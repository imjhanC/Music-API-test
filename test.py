from fastapi import FastAPI, Query, HTTPException
from yt_dlp import YoutubeDL
import yt_dlp
import uvicorn 
from fastapi.middleware.cors import CORSMiddleware
import random
import time
import asyncio
from typing import Optional

app = FastAPI()

# Add CORS middleware to allow requests from React Native app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins for development
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

class MusicSearcher:
    def __init__(self):
        # Expanded list of user agents (real browser user agents)
        self.user_agents = [
            'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36'
        ]
        
        # Cache for stream URLs to avoid repeated requests
        self.url_cache = {}
        self.cache_expiry = {}
        self.last_request_time = {}
    
    def format_duration_fast(self, duration):
        """Format duration in seconds to MM:SS or HH:MM:SS format"""
        if not duration or duration <= 0:
            return "0:00"
        
        hours = int(duration // 3600)
        minutes = int((duration % 3600) // 60)
        seconds = int(duration % 60)
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes}:{seconds:02d}"
    
    def format_views_fast(self, view_count):
        """Format view count to human readable format"""
        if not view_count:
            return "0 views"
        
        if view_count >= 1_000_000_000:
            return f"{view_count / 1_000_000_000:.1f}B views"
        elif view_count >= 1_000_000:
            return f"{view_count / 1_000_000:.1f}M views"
        elif view_count >= 1_000:
            return f"{view_count / 1_000:.1f}K views"
        else:
            return f"{view_count} views"
    
    def rate_limit(self, key: str, delay: float = 2.0):
        """Simple rate limiting"""
        now = time.time()
        if key in self.last_request_time:
            elapsed = now - self.last_request_time[key]
            if elapsed < delay:
                time.sleep(delay - elapsed)
        self.last_request_time[key] = time.time()
    
    def perform_search(self, query, offset=0, exclude_ids=None, batch_size=10):
        """Perform the actual search using yt-dlp - now supports pagination and exclusion."""
        if not query:
            return []
        
        if exclude_ids is None:
            exclude_ids = set()
        else:
            exclude_ids = set(exclude_ids)
        
        try:
            print(f"Searching for: {query} (offset={offset}, exclude={len(exclude_ids)})")
            
            # Enhanced yt-dlp options with better anti-detection
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
                'format': 'best',
                # Add headers to avoid detection
                'http_headers': {
                    'User-Agent': random.choice(self.user_agents),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Cache-Control': 'max-age=0',
                },
            }
            
            # Rate limiting for searches
            self.rate_limit('search', 1.0)
            
            # Fetch more results than needed to allow for exclusion
            fetch_count = max(batch_size * 2, 30)
            
            with yt_dlp.YoutubeDL(search_opts) as ydl:
                search_results = ydl.extract_info(
                    f"ytsearch{fetch_count + offset}:{query}",
                    download=False
                )
            
            print(f"yt-dlp search response received")
            
            if not search_results or 'entries' not in search_results:
                print("No entries in search results")
                return []
            
            entries = search_results.get('entries', [])
            
            # Skip offset and filter out excluded IDs
            filtered = []
            seen = set()
            
            for entry in entries[offset:]:
                if not entry or not entry.get('id'):
                    continue
                
                vid = entry['id']
                if vid in exclude_ids or vid in seen:
                    continue
                
                seen.add(vid)
                title = entry.get('title', 'No Title')
                uploader = entry.get('uploader', 'Unknown')
                duration = entry.get('duration')
                view_count = entry.get('view_count')
                
                # Skip if title and uploader are the same (after case-insensitive comparison)
                if str(title).strip().lower() == str(uploader).strip().lower():
                    continue
                
                # Skip if duration is 0 or invalid
                if not duration or duration <= 0:
                    continue
                
                result = {
                    'title': str(title).strip()[:100],
                    'thumbnail_url': f"https://img.youtube.com/vi/{vid}/mqdefault.jpg",
                    'videoId': vid,
                    'uploader': str(uploader).strip()[:50] if uploader else 'Unknown',
                    'duration': self.format_duration_fast(duration),
                    'view_count': self.format_views_fast(view_count),
                    'url': f"https://www.youtube.com/watch?v={vid}"
                }
                filtered.append(result)
                
                if len(filtered) >= batch_size:
                    break
            
            print(f"Processed {len(filtered)} results (offset={offset})")
            return filtered
            
        except Exception as e:
            print(f"yt-dlp search failed: {str(e)}")
            return []
    
    def get_stream_info(self, video_id_or_url, retry_count=0):
        """Get streaming URL with enhanced error handling and fallback strategies"""
        try:
            # Check cache first
            cache_key = video_id_or_url
            if cache_key in self.url_cache and cache_key in self.cache_expiry:
                if time.time() < self.cache_expiry[cache_key]:
                    print(f"Using cached URL for {video_id_or_url}")
                    return self.url_cache[cache_key]
            
            # Rate limiting for stream requests
            self.rate_limit('stream', 3.0)
            
            # Get a random user agent
            user_agent = random.choice(self.user_agents)
            
            if not video_id_or_url.startswith('http'):
                url = f"https://www.youtube.com/watch?v={video_id_or_url}"
            else:
                url = video_id_or_url
            
            print(f"Extracting stream info for: {url} (attempt {retry_count + 1})")
            
            # Multiple extraction strategies
            strategies = [
                # Strategy 1: Mobile-optimized extraction
                {
                    'format': 'bestaudio[acodec^=mp4a]/bestaudio[ext=m4a]/bestaudio[acodec^=aac]/bestaudio',
                    'quiet': True,
                    'no_warnings': True,
                    'ignoreerrors': True,
                    'geo_bypass': True,
                    'socket_timeout': 25,
                    'retries': 4,
                    'noplaylist': True,
                    'youtube_include_dash_manifest': False,
                    'youtube_include_hls_manifest': False,
                    'prefer_insecure': False,
                    'http_headers': {
                        'User-Agent': user_agent,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1',
                        'Sec-Fetch-Dest': 'document',
                        'Sec-Fetch-Mode': 'navigate',
                        'Sec-Fetch-Site': 'cross-site',
                        'Sec-Fetch-User': '?1',
                        'Cache-Control': 'no-cache',
                        'Pragma': 'no-cache',
                    },
                },
                # Strategy 2: Fallback with different format
                {
                    'format': 'worst[acodec!=none]/bestaudio/best',
                    'quiet': True,
                    'no_warnings': True,
                    'ignoreerrors': True,
                    'geo_bypass': True,
                    'socket_timeout': 20,
                    'retries': 3,
                    'noplaylist': True,
                    'youtube_include_dash_manifest': False,
                    'youtube_include_hls_manifest': False,
                    'http_headers': {
                        'User-Agent': user_agent,
                        'Accept': '*/*',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate',
                        'DNT': '1',
                        'Connection': 'keep-alive',
                    },
                }
            ]
            
            for i, strategy in enumerate(strategies):
                try:
                    print(f"Trying strategy {i + 1}")
                    
                    with yt_dlp.YoutubeDL(strategy) as ydl:
                        info = ydl.extract_info(url, download=False)
                        
                        if not info:
                            print(f"No info extracted from video (strategy {i + 1})")
                            continue
                        
                        # Try to get the direct URL from the info dict first
                        audio_url = info.get('url')
                        
                        # If it's a playlist/manifest URL, try to get from formats
                        if not audio_url or any(x in audio_url.lower() for x in ['m3u8', 'manifest', 'playlist']):
                            formats = info.get('formats', [])
                            
                            # Filter for direct audio URLs (no HLS/manifests)
                            audio_formats = []
                            for fmt in formats:
                                fmt_url = fmt.get('url', '')
                                if (fmt.get('acodec') != 'none' and 
                                    fmt_url and
                                    not any(x in fmt_url.lower() for x in ['m3u8', 'manifest', 'playlist']) and
                                    not fmt.get('is_hls') and
                                    not fmt.get('protocol') in ['m3u8', 'm3u8_native', 'hls']):
                                    
                                    # Prefer audio-only formats
                                    if fmt.get('vcodec') == 'none' or any(ext in fmt_url.lower() for ext in ['.m4a', '.mp3', '.aac', '.ogg', '.wav']):
                                        audio_formats.append(fmt)
                            
                            # Sort by quality/bitrate (prefer higher quality)
                            audio_formats.sort(key=lambda x: (
                                x.get('abr', 0) or x.get('tbr', 0),
                                1 if x.get('vcodec') == 'none' else 0  # Prefer audio-only
                            ), reverse=True)
                            
                            if audio_formats:
                                best_format = audio_formats[0]
                                audio_url = best_format.get('url')
                                print(f"Selected format: {best_format.get('ext')}, bitrate: {best_format.get('abr')}, vcodec: {best_format.get('vcodec')}")
                        
                        if not audio_url:
                            print(f"No audio URL found (strategy {i + 1})")
                            continue
                        
                        # Final validation to ensure it's not a playlist
                        if any(x in audio_url.lower() for x in ['m3u8', 'manifest', 'playlist']):
                            print(f"Still got playlist URL (strategy {i + 1}): {audio_url[:100]}...")
                            continue
                        
                        # Prepare result
                        result = {
                            'title': info.get('title'),
                            'url': audio_url,
                            'duration': self.format_duration_fast(info.get('duration')),
                            'uploader': info.get('uploader'),
                            'thumbnail': info.get('thumbnail'),
                            'view_count': self.format_views_fast(info.get('view_count')),
                            'format': info.get('ext'),
                            'strategy_used': i + 1
                        }
                        
                        # Cache the result (expire in 1 hour)
                        self.url_cache[cache_key] = result
                        self.cache_expiry[cache_key] = time.time() + 3600
                        
                        print(f"Successfully extracted stream using strategy {i + 1}")
                        return result
                        
                except Exception as strategy_error:
                    print(f"Strategy {i + 1} failed: {str(strategy_error)}")
                    continue
            
            # If all strategies failed, try one more time with a different video ID format
            if retry_count < 2 and not video_id_or_url.startswith('http'):
                print("All strategies failed, trying with different URL format...")
                time.sleep(2)  # Brief delay before retry
                return self.get_stream_info(f"https://music.youtube.com/watch?v={video_id_or_url}", retry_count + 1)
            
            print("All extraction strategies failed")
            return None
                
        except Exception as e:
            print(f"Stream extraction failed: {str(e)}")
            
            # Retry logic for certain errors
            if retry_count < 2 and any(keyword in str(e).lower() for keyword in ['timeout', 'connection', 'network']):
                print(f"Retrying due to network error... (attempt {retry_count + 2})")
                time.sleep(3 * (retry_count + 1))  # Exponential backoff
                return self.get_stream_info(video_id_or_url, retry_count + 1)
            
            return None

# Create global searcher instance
searcher = MusicSearcher()

@app.get("/search")
def search_music(
    query: str = Query(..., description="Search query for music"),
    offset: int = Query(0, description="Offset for pagination", ge=0),
    batch_size: int = Query(10, description="Number of results to return", ge=1, le=50),
    exclude_ids: str = Query("", description="Comma-separated video IDs to exclude")
):
    """Search for music using the enhanced yt-dlp approach"""
    try:
        # Parse exclude_ids
        excluded = []
        if exclude_ids:
            excluded = [vid.strip() for vid in exclude_ids.split(',') if vid.strip()]
        
        results = searcher.perform_search(
            query=query,
            offset=offset,
            exclude_ids=excluded,
            batch_size=batch_size
        )
        
        return {
            "results": results,
            "query": query,
            "offset": offset,
            "batch_size": batch_size,
            "count": len(results)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.get("/stream")
def stream_music(
    video_id: str = Query(..., description="YouTube video ID or full URL")
):
    """Get streaming URL for a video with enhanced error handling"""
    try:
        print(f"Stream request received for: {video_id}")
        stream_info = searcher.get_stream_info(video_id)
        
        if not stream_info:
            # Try alternative search if direct extraction fails
            print(f"Direct extraction failed, trying alternative approaches...")
            
            # Try searching for the video ID to get alternative URLs
            search_results = searcher.perform_search(f"site:youtube.com {video_id}", batch_size=5)
            if search_results:
                for result in search_results:
                    if result['videoId'] == video_id:
                        # Try extracting from the search result URL
                        stream_info = searcher.get_stream_info(result['url'])
                        if stream_info:
                            break
            
            if not stream_info:
                raise HTTPException(
                    status_code=404, 
                    detail="Stream not found or unavailable. This video may be region-locked or age-restricted."
                )
        
        return stream_info
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error in stream endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Stream extraction failed: {str(e)}")

@app.get("/stream/{video_id}")
def stream_music_path(video_id: str):
    """Alternative endpoint with video ID in path"""
    return stream_music(video_id)

@app.get("/clear_cache")
def clear_cache():
    """Clear the URL cache"""
    searcher.url_cache.clear()
    searcher.cache_expiry.clear()
    return {"message": "Cache cleared successfully"}

@app.get("/debug/formats/{video_id}")
def debug_formats(video_id: str):
    """Debug endpoint to see available formats"""
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        
        debug_opts = {
            'listformats': True,
            'quiet': True,
            'no_warnings': True,
            'http_headers': {
                'User-Agent': random.choice(searcher.user_agents),
            },
        }
        
        with yt_dlp.YoutubeDL(debug_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            
        formats = info.get('formats', [])
        
        # Filter and format for readability
        audio_formats = []
        for fmt in formats:
            if fmt.get('acodec') != 'none':
                audio_formats.append({
                    'format_id': fmt.get('format_id'),
                    'ext': fmt.get('ext'),
                    'acodec': fmt.get('acodec'),
                    'vcodec': fmt.get('vcodec'),
                    'abr': fmt.get('abr'),
                    'url': fmt.get('url')[:100] + '...' if fmt.get('url') else None,
                    'protocol': fmt.get('protocol'),
                    'is_hls': fmt.get('is_hls'),
                })
        
        return {
            'video_id': video_id,
            'title': info.get('title'),
            'audio_formats': audio_formats,
            'total_formats': len(formats)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Debug failed: {str(e)}")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy", 
        "message": "Enhanced Music API is running",
        "cache_size": len(searcher.url_cache)
    }

@app.get("/")
def root():
    """API information"""
    return {
        "name": "Enhanced YouTube Music Search API",
        "version": "2.0.0",
        "endpoints": {
            "search": "/search?query=song+name&offset=0&batch_size=10",
            "stream": "/stream?video_id=VIDEO_ID",
            "stream_alt": "/stream/VIDEO_ID",
            "debug": "/debug/formats/VIDEO_ID",
            "clear_cache": "/clear_cache",
            "health": "/health"
        },
        "description": "Enhanced YouTube music search and streaming API with anti-detection measures"
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)