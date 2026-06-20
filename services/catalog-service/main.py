from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Float, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List
import os
from dotenv import load_dotenv

load_dotenv()

# ===================== CONFIG =====================
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "mysql+pymysql://root:68686868@mysql:3306/food"
)

# ===================== DATABASE SETUP =====================
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ProductModel(Base):
    __tablename__ = "home_product"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    image_url = Column(String(500))
    description = Column(Text)
    price = Column(Float, nullable=False)
    stock_quantity = Column(Integer, default=0)
    category = Column(String(50))  # 'food', 'drink', 'fastfood'

# ===================== PYDANTIC MODELS =====================
class ProductResponse(BaseModel):
    id: int
    name: str
    image_url: str = None
    description: str = None
    price: float
    stock_quantity: int
    category: str = None
    
    class Config:
        from_attributes = True

class ProductDetailResponse(ProductResponse):
    pass

class ProductCreateRequest(BaseModel):
    name: str
    price: float
    stock_quantity: int = 0
    image_url: str = None
    description: str = None
    category: str = None

# ===================== FASTAPI APP =====================
app = FastAPI(
    title="MIXI Catalog Service",
    description="Product Catalog Service",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ===================== ENDPOINTS =====================
@app.get("/api/catalog/products", response_model=List[ProductResponse])
async def get_products(
    category: str = None, 
    skip: int = 0, 
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get all products with optional category filter"""
    query = db.query(ProductModel)
    
    if category:
        query = query.filter(ProductModel.category == category)
    
    products = query.offset(skip).limit(limit).all()
    return products

@app.get("/api/catalog/products/{product_id}", response_model=ProductDetailResponse)
async def get_product(product_id: int, db: Session = Depends(get_db)):
    """Get product details by ID"""
    product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    return product

@app.post("/api/catalog/products", response_model=ProductResponse)
async def create_product(product: ProductCreateRequest, db: Session = Depends(get_db)):
    """Create new product"""
    db_product = ProductModel(**product.dict())
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product

@app.put("/api/catalog/products/{product_id}", response_model=ProductResponse)
async def update_product(
    product_id: int, 
    product_data: ProductCreateRequest,
    db: Session = Depends(get_db)
):
    """Update product"""
    product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    for key, value in product_data.dict().items():
        setattr(product, key, value)
    
    db.commit()
    db.refresh(product)
    return product

@app.delete("/api/catalog/products/{product_id}")
async def delete_product(product_id: int, db: Session = Depends(get_db)):
    """Delete product"""
    product = db.query(ProductModel).filter(ProductModel.id == product_id).first()
    
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    
    db.delete(product)
    db.commit()
    return {"message": "Product deleted successfully"}

@app.get("/api/catalog/categories")
async def get_categories(db: Session = Depends(get_db)):
    """Get all product categories"""
    categories = db.query(ProductModel.category).distinct().all()
    return {"categories": [c[0] for c in categories if c[0]]}

@app.get("/api/catalog/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "service": "Catalog Service"}

@app.get("/health")
async def root_health():
    """Root health endpoint"""
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
