import yt_dlp
import threading
from typing import Dict, List, Optional
from fastapi import HTTPException


class SearchHelper:
    """Helper class for YouTube search and stream URL extraction"""
    
    @staticmethod
    def format_duration_fast(seconds):
        """Format duration from seconds to MM:SS or HH:MM:SS"""
        if not seconds or seconds <= 0:
            return "0:00"
        
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{secs:02d}"
        return f"{minutes}:{secs:02d}"
    
    @staticmethod
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
        return f"{view_count:,} views"
    
    @staticmethod
    def get_common_headers():
        """Get common HTTP headers for yt-dlp"""
        return {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-us,en;q=0.5',
            'Sec-Fetch-Mode': 'navigate',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    @classmethod
    def perform_search(cls, query: str, limit: Optional[int] = None) -> List[Dict]:
        """Perform YouTube search using yt-dlp with maximum results possible - OPTIMIZED"""
        if not query:
            return []
        
        try:
            thread_name = threading.current_thread().name
            print(f"[{thread_name}] Searching for: {query}")
            
            # Optimized yt-dlp options for maximum speed
            search_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': 'in_playlist',  # Even faster extraction
                'skip_download': True,
                'ignoreerrors': True,
                'geo_bypass': True,
                'noplaylist': True,
                'socket_timeout': 8,  # Reduced from 10 for faster timeouts
                'retries': 1,  # Reduced from 2 - one retry is enough
                'format': 'best',
                'http_headers': cls.get_common_headers(),
                'nocheckcertificate': True,  # Skip cert verification for speed
                'no_color': True,  # Disable color codes
                'extractor_args': {
                    'youtube': {
                        'skip': ['hls', 'dash', 'translated_subs']  # Skip unnecessary data
                    }
                }
            }
            
            # Fetch maximum results: use limit if provided, otherwise fetch 200
            fetch_count = limit if limit else 20
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
            
            # Pre-allocate list with estimated size for better performance
            filtered = []
            filtered_reserve = limit if limit else min(len(entries), 200)
            
            # Use set for O(1) lookup instead of list
            seen = set()
            
            # Optimized processing loop with early exit
            target_limit = limit if limit else 20
            
            for entry in entries:
                # Fast skip invalid entries
                if not entry:
                    continue
                    
                vid = entry.get('id')
                if not vid or vid in seen:
                    continue
                    
                seen.add(vid)
                
                # Fast access with get() and defaults
                title = entry.get('title', 'No Title')
                uploader = entry.get('uploader', 'Unknown')
                duration = entry.get('duration')
                view_count = entry.get('view_count')
                
                # Build result dict directly without intermediate variables
                filtered.append({
                    'title': str(title)[:100],  # Combine strip and slice
                    'thumbnail_url': f"https://img.youtube.com/vi/{vid}/mqdefault.jpg",
                    'videoId': vid,
                    'uploader': str(uploader)[:50] if uploader else 'Unknown',
                    'duration': cls.format_duration_fast(duration) if duration else 'Live/Unknown',
                    'view_count': cls.format_views_fast(view_count),
                    'url': f"https://www.youtube.com/watch?v={vid}"
                })
                
                # Early exit when limit reached
                if len(filtered) >= target_limit:
                    break
            
            print(f"[{thread_name}] Processed {len(filtered)} results")
            return filtered
            
        except Exception as e:
            thread_name = threading.current_thread().name
            print(f"[{thread_name}] yt-dlp search failed: {str(e)}")
            return []
    
    @classmethod
    def get_audio_stream_url(cls, video_id: str) -> Dict:
        """Get streaming URL for audio - ENFORCES MP3 FORMAT ONLY"""
        try:
            thread_name = threading.current_thread().name
            youtube_url = f"https://www.youtube.com/watch?v={video_id}"
            
            print(f"[{thread_name}] Processing video_id: {video_id} - ENFORCING MP3 FORMAT")
            
            # Force MP3 format only with postprocessor
            opts = {
                'format': 'bestaudio[ext=webm]/bestaudio[ext=m4a]/bestaudio',
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '320',
                }],
                'quiet': True,
                'no_warnings': True,
                'extractor_retries': 1,
                'fragment_retries': 1,
                'socket_timeout': 15,
                'http_headers': cls.get_common_headers()
            }
            
            print(f"[{thread_name}] Extracting MP3 audio stream for {video_id}")
            
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(youtube_url, download=False)
                
                if info and info.get('url'):
                    # Get audio quality information
                    quality_info = "320kbps MP3"
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
                        'format': 'mp3',
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
    
    @classmethod
    def get_video_stream_url(cls, video_id: str) -> Dict:
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
                'socket_timeout': 20,
                'http_headers': cls.get_common_headers()
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
                            # Safe handling of None values
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
                            # Safe handling of None values
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