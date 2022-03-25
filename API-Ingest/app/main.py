# You need this to use FastAPI, work with statuses and be able to end HTTPExceptions
from fastapi import FastAPI, status, HTTPException
 
# You need this to be able to turn classes into JSONs and return
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# Needed for json.dumps
import json

# Both used for BaseModel
from pydantic import BaseModel

from datetime import datetime
from kafka import KafkaProducer, producer



# Create class (schema) for the JSON
# Date get's ingested as string and then before writing validated
class VideoItem(BaseModel):
    video_id: str
    trending_date: str
    title: str
    channel_title: str
    category_id: int
    publish_time: str
    views: int
    likes: int
    dislikes: int
    comment_count: int

# This is important for general execution and the docker later
app = FastAPI()

# Base URL
@app.get("/")
async def root():
    return {"message": "Hello World!"}

# Add a new video
@app.post("/videoitem")
async def post_videoitem(item: VideoItem): #body awaits a json with video item information
    print("Message received")
    try:
        # Evaluate the timestamp and parse it to datetime object you can work with
        date = datetime.strptime(item.trending_date, "%y.%d.%m")

        print('Found the first timestamp: ', date)

        # Replace strange date with new datetime
        # Use strftime to parse the string in the right format 
        item.trending_date = date.strftime("%Y-%m-%d")
        print("New trending date:", item.trending_date)
        
        # Parse item back to json
        json_of_item = jsonable_encoder(item)
        
        # Dump the json out as string
        json_as_string = json.dumps(json_of_item)
        print(json_as_string)
        
        # Produce the string
        produce_kafka_string(json_as_string)

        # Encode the created customer item if successful into a JSON and return it to the client with 201
        return JSONResponse(content=json_of_item, status_code=201)
    
    # Will be thrown by datetime if the date does not fit
    # All other value errors are automatically taken care of because of the video Class
    except ValueError:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)
        

def produce_kafka_string(json_as_string):
    # Create producer
        producer = KafkaProducer(bootstrap_servers='kafka:9092', acks =1)
        
        # Write the string as bytes because Kafka needs it this way
        producer.send('ingestion-topic', bytes(json_as_string, 'utf-8'))
        producer.flush() 

