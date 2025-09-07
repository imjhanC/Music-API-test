from fastapi import FastAPI, Query
from yt_dlp import YoutubeDL

app = FastAPI()

@app.get("/search")
def search_music(query: str = Query(..., description="Search query for music")):
    ydl_opts = {
        'format': 'bestaudio/best',
        'noplaylist': True,
        'quiet': True,
        'default_search': 'ytsearch5',  # search top 5 results
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(query, download=False)
        return {"results": info['entries']}

@app.get("/stream")
def stream_music(url: str):
    ydl_opts = {
        'format': 'bestaudio/best',
        'quiet': True
    }
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)
        return {"title": info['title'], "url": info['url']}


# uvicorn app:app --reload
