from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic_settings import BaseSettings, SettingsConfigDict
import csv
import json

class Settings(BaseSettings):
    api_base_url: str
    model_config = SettingsConfigDict(env_file=".env")


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

settings = Settings()
app = FastAPI()
manager = ConnectionManager()
map_reduce_job = []

templates = Jinja2Templates(directory="pages")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/" , response_class=HTMLResponse)
async def root_dir(request: Request):    
    return templates.TemplateResponse("main.html", {"request" : request, 
                                                    "data" : {
                                                        "api_base_url" : settings.api_base_url
                                                        }
                                                    })

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # print(websocket.client)
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
    except:
        manager.disconnect(websocket)

@app.post("/update/{number}")
async def update_timestamps(number: int):
    map_reduce_job.append(number)

    broadcast_message =  f'MapReduce job terminated at {map_reduce_job[2]}' if len(map_reduce_job) == 3 else f'MapReduce job initiated at {map_reduce_job[0]}'
    
    if len(map_reduce_job) != 2:
        await manager.broadcast(json.dumps({
            "type" : "notification",
            "message": broadcast_message
            }))


    if len(map_reduce_job) == 3:

        with open("./static/mapred.csv", "a", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(map_reduce_job)
        
        await manager.broadcast(json.dumps({
            "type" : "data",
            "data" : map_reduce_job
            }))
        
        map_reduce_job.clear()

    
    return number