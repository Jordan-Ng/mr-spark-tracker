from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic_settings import BaseSettings, SettingsConfigDict
import csv
import json
import time
import datetime

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
spark_job =[]

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

@app.post("/update/{job_type}/{timestamp}")
async def update_timestamps(job_type: str, timestamp: str):

    job_mapper = {
        "mr": map_reduce_job,
        "spark": spark_job
    }

    file_mapper = {
        "mr" : "mapred",
        "spark" : "spark"
    }

    job_mapper[job_type].append(datetime.datetime.fromtimestamp(float(timestamp)))

    broadcast_message =  f'MapReduce job terminated at {job_mapper[job_type][1]}' if len(job_mapper[job_type]) == 2 else f'MapReduce job initiated at {job_mapper[job_type][0]}'
    
    await manager.broadcast(json.dumps({
        "type" : "notification",
        "message": broadcast_message
        }))

    if len(job_mapper[job_type]) == 2:
        start, end = [job_mapper[job_type][0], job_mapper[job_type][1]]
        elapsed_time = (end - start) / datetime.timedelta(milliseconds=1)
        new_row = [start.strftime("%d/%m/%Y - %H:%M:%S").strip(' \" '), end.strftime("%d/%m/%Y - %H:%M:%S").strip('\"'), elapsed_time]

        with open(f'./static/{file_mapper[job_type]}.csv', "a", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(new_row)
        
        await manager.broadcast(json.dumps({
            "type" : "data",
            "data" : new_row
            }))
        
        map_reduce_job.clear()

    
    return timestamp