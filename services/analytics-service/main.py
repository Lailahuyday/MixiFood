from fastapi import FastAPI
import random
from datetime import datetime, timedelta

app = FastAPI(title="MIXI Analytics Service")

@app.get("/api/analytics/stats")
async def get_stats():
    # Mock real-time stats
    return {
        "total_events": random.randint(1000, 5000),
        "active_users": random.randint(50, 200),
        "orders_today": random.randint(10, 50),
        "revenue": random.uniform(1000, 5000)
    }

@app.get("/api/analytics/events-timeline")
async def get_timeline():
    # Mock timeline data
    now = datetime.now()
    return [
        {"timestamp": (now - timedelta(minutes=i)).isoformat(), "count": random.randint(5, 20)}
    ]

@app.get("/api/analytics/health")
async def health():
    return {"status": "ok", "service": "Analytics Service"}

@app.get("/health")
async def root_health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
