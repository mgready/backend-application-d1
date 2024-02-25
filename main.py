from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from pymongo import MongoClient
from datetime import datetime
from typing import List
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import uvicorn

app = FastAPI()


origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


mongoclient = MongoClient('mongodb://localhost:27017/')  # Connect to MongoDB running on localhost
db = mongoclient['sensor_data']  # Select the database
collection = db['sensor_readings']  # Select the collection

async def insert_data(data):
    await asyncio.sleep(30)  # Simulate an async operation, e.g., a slow database insertion
    print(data)
    collection.insert_one(data)

@app.get("/message/")
async def sendMessage(message: str):
    print(message)
    return {"message": message}

@app.get("/data", response_model=List[dict])
async def get_all_data(quantity:int):
    try:
        data_list = list(collection.find({}))
        for data in data_list:
            data["_id"] = str(data["_id"])
        return data_list[-5:]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/data")
async def receive_data(request: Request, background_tasks: BackgroundTasks):
    data = await request.json()
    data["timestamp"] = datetime.utcnow()
    background_tasks.add_task(insert_data, data)
    return {"message": "Data received. Insertion into MongoDB will be processed in the background."}
if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=2000)