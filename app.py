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
import requests
from bs4 import BeautifulSoup
import json
import re

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

def get_random_user_agent():
    """Return a random user agent to avoid detection"""
    user_agents = [
        # Windows Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        
        # Mac Chrome
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        
        # Firefox
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
        
        # Safari
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        
        # Mobile
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36"
    ]
    return random.choice(user_agents)

def get_random_headers():
    """Return random headers to avoid detection"""
    return {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }

def perform_search_sync(query: str) -> List[Dict]:
    """Perform YouTube search using direct HTTP requests"""
    if not query:
        return []
    
    try:
        print(f"Searching for: {query}")
        
        # Prepare headers to mimic a browser
        headers = get_random_headers()
        
        # Search YouTube
        search_url = f"https://www.youtube.com/results?search_query={requests.utils.quote(query)}"
        response = requests.get(search_url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            print(f"Search request failed with status code: {response.status_code}")
            return []
        
        # Parse the HTML response
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find script tags containing video data
        scripts = soup.find_all('script')
        video_data = []
        
        for script in scripts:
            script_text = script.text
            if 'var ytInitialData' in script_text:
                try:
                    # Extract the JSON data
                    json_text = script_text.split('var ytInitialData = ')[1].split(';')[0]
                    data = json.loads(json_text)
                    
                    # Extract video information
                    contents = data.get('contents', {}).get('twoColumnSearchResultsRenderer', {}).get('primaryContents', {}).get('sectionListRenderer', {}).get('contents', [])
                    
                    for content in contents:
                        item_section = content.get('itemSectionRenderer', {})
                        for item in item_section.get('contents', []):
                            if 'videoRenderer' in item:
                                video = item['videoRenderer']
                                video_id = video.get('videoId')
                                if not video_id:
                                    continue
                                    
                                title = video.get('title', {}).get('runs', [{}])[0].get('text', 'No Title')
                                uploader = video.get('ownerText', {}).get('runs', [{}])[0].get('text', 'Unknown')
                                view_count = video.get('viewCountText', {}).get('simpleText', '0')
                                duration = video.get('lengthText', {}).get('simpleText', '0:00')
                                thumbnails = video.get('thumbnail', {}).get('thumbnails', [])
                                thumbnail = thumbnails[0].get('url', f'https://img.youtube.com/vi/{video_id}/mqdefault.jpg') if thumbnails else f'https://img.youtube.com/vi/{video_id}/mqdefault.jpg'
                                
                                # Convert duration to seconds
                                duration_parts = duration.split(':')
                                if len(duration_parts) == 2:
                                    duration_seconds = int(duration_parts[0]) * 60 + int(duration_parts[1])
                                elif len(duration_parts) == 3:
                                    duration_seconds = int(duration_parts[0]) * 3600 + int(duration_parts[1]) * 60 + int(duration_parts[2])
                                else:
                                    duration_seconds = 0
                                
                                # Convert view count to number
                                view_count_num = 0
                                view_count_text = view_count.lower()
                                try:
                                    if 'b' in view_count_text:
                                        view_count_num = int(float(view_count_text.replace('b', '').replace(',', '')) * 1_000_000_000)
                                    elif 'm' in view_count_text:
                                        view_count_num = int(float(view_count_text.replace('m', '').replace(',', '')) * 1_000_000)
                                    elif 'k' in view_count_text:
                                        view_count_num = int(float(view_count_text.replace('k', '').replace(',', '')) * 1_000)
                                    else:
                                        view_count_num = int(view_count_text.replace(',', '').replace(' views', '').replace(' view', ''))
                                except:
                                    view_count_num = 0
                                
                                # Skip if title and uploader are the same (likely not music)
                                if str(title).strip().lower() == str(uploader).strip().lower():
                                    continue
                                    
                                # Skip if duration is 0 or invalid
                                if not duration_seconds or duration_seconds <= 0:
                                    continue
                                
                                # Skip very long videos (likely not songs)
                                if duration_seconds > 600:  # 10 minutes
                                    continue
                                
                                result = {
                                    'title': str(title).strip()[:100],
                                    'thumbnail_url': thumbnail,
                                    'videoId': video_id,
                                    'uploader': str(uploader).strip()[:50] if uploader else 'Unknown',
                                    'duration': duration,
                                    'view_count': format_views_fast(view_count_num),
                                    'url': f"https://www.youtube.com/watch?v={video_id}"
                                }
                                video_data.append(result)
                                
                                # Limit to 5 results
                                if len(video_data) >= 5:
                                    break
                        if len(video_data) >= 5:
                            break
                    if len(video_data) >= 5:
                        break
                except Exception as e:
                    print(f"Error parsing JSON data: {e}")
                    continue
        
        print(f"Processed {len(video_data)} results")
        return video_data
        
    except Exception as e:
        print(f"Search failed: {str(e)}")
        # Fallback to yt-dlp if direct search fails
        return perform_search_fallback(query)

def perform_search_fallback(query: str) -> List[Dict]:
    """Fallback search using yt-dlp if direct search fails"""
    try:
        print("Using fallback search method")
        
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
            'http_headers': get_random_headers()
        }
        
        # Fetch 10 results to filter down to 5 good ones
        fetch_count = 10
        with yt_dlp.YoutubeDL(search_opts) as ydl:
            search_results = ydl.extract_info(
                f"ytsearch{fetch_count}:{query}",
                download=False
            )
        
        print(f"yt-dlp fallback response received")
        
        if not search_results or 'entries' not in search_results:
            print("No entries in fallback search results")
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
        
        print(f"Fallback processed {len(filtered)} results")
        return filtered
        
    except Exception as e:
        print(f"Fallback search also failed: {str(e)}")
        return []

def get_stream_url_sync(video_id: str) -> Dict:
    """Get streaming URL for a specific video with enhanced bot detection avoidance"""
    try:
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"
        
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
                'http_headers': get_random_headers()
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
                'http_headers': get_random_headers()
            },
            
            # Strategy 4: Minimalist approach
            {
                'format': 'bestaudio/best',
                'quiet': True,
                'http_headers': get_random_headers()
            }
        ]
        
        last_error = None
        
        for i, opts in enumerate(strategies):
            try:
                print(f"Trying extraction strategy {i+1}")
                
                # Add small random delay to avoid rate limiting
                if i > 0:
                    time.sleep(random.uniform(1, 3))
                
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
        
        # If all strategies failed, try one more approach with cookies
        try:
            print("Trying final approach with cookie consent bypass")
            final_opts = {
                'format': 'bestaudio[abr>0]/bestaudio/best',
                'quiet': True,
                'no_warnings': True,
                'http_headers': get_random_headers(),
                'cookiefile': None,  # Try without cookies first
                'ignoreerrors': True,
            }
            
            with yt_dlp.YoutubeDL(final_opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                
                if info and info.get('url'):
                    print("Successfully extracted with final approach")
                    return {
                        'stream_url': info['url'],
                        'title': info.get('title', 'Unknown Title'),
                        'duration': info.get('duration', 0),
                        'thumbnail_url': f"https://img.youtube.com/vi/{video_id}/mqdefault.jpg"
                    }
        except Exception as e:
            print(f"Final approach also failed: {str(e)}")
        
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
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Add random delay between attempts to avoid rate limiting
            if attempt > 0:
                await asyncio.sleep(random.uniform(2, 5))
            
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