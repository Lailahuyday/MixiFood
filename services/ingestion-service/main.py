from fastapi import FastAPI, HTTPException, Depends, Header, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from kafka import KafkaProducer
from jose import JWTError, jwt
import json
import os
import time
from typing import Optional, Dict, Any
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()

# ===================== CONFIG =====================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"

# ===================== LOGGING =====================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ===================== FASTAPI APP =====================
app = FastAPI(
    title="MIXI Ingestion Service",
    description="Real-time Event Ingestion Service with Kafka Producer",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===================== KAFKA PRODUCER =====================
producer = None

def get_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                retries=5,
                acks='all',
                compression_type='gzip'
            )
            logger.info(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {e}")
    return producer

# ===================== PYDANTIC MODELS =====================
class EventPayload(BaseModel):
    action: str = Field(..., description="Event action: login, view, add_to_cart, checkout, etc")
    user_id: int
    product_id: Optional[int] = None
    quantity: Optional[int] = None
    data: Dict[str, Any] = Field(default_factory=dict)

class EventResponse(BaseModel):
    status: str
    message: str
    event_id: str
    topic: str

class HealthResponse(BaseModel):
    status: str
    service: str
    kafka_connected: bool

# ===================== AUTHENTICATION =====================
async def verify_token(authorization: Optional[str] = Header(None)):
    """Verify JWT token from Authorization header"""
    if not authorization:
        return None
    
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            return None
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        return user_id
    except (JWTError, ValueError):
        return None

# ===================== ENDPOINTS =====================
@app.post("/api/events/ingest", response_model=EventResponse)
async def ingest_event(
    event: EventPayload,
    user_id: int = Depends(verify_token)
):
    """
    Ingest real-time event into Kafka stream.
    Routes events to appropriate topic based on action type.
    """
    p = get_producer()
    if p is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka not available"
        )
    
    try:
        # Determine topic based on action
        topic_mapping = {
            "login": "user_events",
            "logout": "user_events",
            "view": "product_events",
            "add_to_cart": "product_events",
            "remove_from_cart": "product_events",
            "checkout": "order_events",
            "payment": "payment_events"
        }
        
        topic = topic_mapping.get(event.action, "general_events")
        
        # Generate event ID
        event_id = f"EVT_{int(time.time() * 1000)}_{event.user_id}"
        
        # Construct complete event log
        log_data = {
            "event_id": event_id,
            "timestamp": datetime.utcnow().isoformat(),
            "action": event.action,
            "user_id": user_id if user_id else event.user_id,
            "product_id": event.product_id,
            "quantity": event.quantity,
            "additional_data": event.data,
            "metadata": {
                "source": "ingestion-service",
                "version": "2.0",
                "ingestion_timestamp": time.time()
            }
        }
        
        # Send to Kafka
        future = p.send(topic, log_data)
        future.get(timeout=10)
        
        logger.info(f"Event {event_id} sent to topic {topic}")
        
        return {
            "status": "success",
            "message": "Event ingested successfully",
            "event_id": event_id,
            "topic": topic
        }
    except Exception as e:
        logger.error(f"Error ingesting event: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to ingest event: {str(e)}"
        )

@app.post("/api/events/batch")
async def ingest_batch_events(
    events: list[EventPayload],
    user_id: int = Depends(verify_token)
):
    """Ingest multiple events in batch"""
    p = get_producer()
    if p is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka not available"
        )
    
    try:
        results = []
        for event in events:
            topic_mapping = {
                "login": "user_events",
                "view": "product_events",
                "add_to_cart": "product_events",
                "checkout": "order_events",
                "payment": "payment_events"
            }
            topic = topic_mapping.get(event.action, "general_events")
            
            event_id = f"EVT_{int(time.time() * 1000)}_{event.user_id}"
            
            log_data = {
                "event_id": event_id,
                "timestamp": datetime.utcnow().isoformat(),
                "action": event.action,
                "user_id": user_id if user_id else event.user_id,
                "product_id": event.product_id,
                "additional_data": event.data,
                "metadata": {"source": "ingestion-service"}
            }
            
            p.send(topic, log_data)
            results.append({"event_id": event_id, "topic": topic, "status": "queued"})
        
        return {
            "status": "success",
            "total_events": len(results),
            "events": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/events/health", response_model=HealthResponse)
async def health():
    """Health check endpoint"""
    p = get_producer()
    kafka_connected = p is not None
    
    return {
        "status": "ok" if kafka_connected else "degraded",
        "service": "Ingestion Service",
        "kafka_connected": kafka_connected
    }

@app.get("/health")
async def root_health():
    """Root health endpoint"""
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
