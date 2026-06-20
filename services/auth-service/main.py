from fastapi import FastAPI, Depends, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv
import json

load_dotenv()

# ===================== CONFIG =====================
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "mysql+pymysql://root:68686868@mysql:3306/food"
)
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
FACEBOOK_CLIENT_ID = os.getenv("FACEBOOK_CLIENT_ID", "")
FACEBOOK_CLIENT_SECRET = os.getenv("FACEBOOK_CLIENT_SECRET", "")

# ===================== DATABASE SETUP =====================
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class UserModel(Base):
    __tablename__ = "auth_users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), unique=True, index=True)
    email = Column(String(255), unique=True, index=True)
    full_name = Column(String(255), nullable=True)
    hashed_password = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    oauth_provider = Column(String(50), nullable=True)  # 'google', 'facebook', 'local'
    oauth_id = Column(String(255), unique=True, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

Base.metadata.create_all(bind=engine)

# ===================== SECURITY =====================
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# ===================== PYDANTIC MODELS =====================
class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: dict

class UserRegister(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(..., min_length=6)
    full_name: str = None

class UserLogin(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    full_name: str = None
    oauth_provider: str = None
    created_at: datetime
    
    class Config:
        from_attributes = True

class GoogleToken(BaseModel):
    token: str

class FacebookToken(BaseModel):
    token: str

# ===================== FASTAPI APP =====================
app = FastAPI(
    title="MIXI Auth Service",
    description="Authentication & Authorization Service with OAuth2",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===================== AUTH DEPENDENCIES =====================
async def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    user = db.query(UserModel).filter(UserModel.id == user_id).first()
    if user is None:
        raise credentials_exception
    return user

# ===================== LOCAL AUTHENTICATION =====================
@app.post("/api/auth/register", response_model=UserResponse)
async def register(user_data: UserRegister, db: Session = Depends(get_db)):
    """Register new user with username and password"""
    # Check if user exists
    existing_user = db.query(UserModel).filter(
        (UserModel.username == user_data.username) | 
        (UserModel.email == user_data.email)
    ).first()
    
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username or email already registered"
        )
    
    # Create new user
    new_user = UserModel(
        username=user_data.username,
        email=user_data.email,
        full_name=user_data.full_name,
        hashed_password=hash_password(user_data.password),
        oauth_provider="local"
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user

@app.post("/api/auth/login", response_model=TokenResponse)
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    """Local login with username and password"""
    user = db.query(UserModel).filter(UserModel.username == form_data.username).first()
    
    if not user or not verify_password(form_data.password, user.hashed_password or ""):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    if not user.is_active:
        raise HTTPException(status_code=400, detail="User account is inactive")
    
    access_token = create_access_token(data={"sub": user.id})
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "full_name": user.full_name
        }
    }

# ===================== GOOGLE OAUTH2 =====================
@app.post("/api/auth/google")
async def google_auth(token_data: GoogleToken, db: Session = Depends(get_db)):
    """Authenticate with Google OAuth2 token"""
    try:
        # Verify token with Google
        google_url = "https://www.googleapis.com/oauth2/v1/userinfo"
        response = requests.get(google_url, params={"access_token": token_data.token})
        
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Invalid Google token")
        
        google_user = response.json()
        google_id = google_user.get("id")
        email = google_user.get("email")
        name = google_user.get("name")
        
        # Find or create user
        user = db.query(UserModel).filter(UserModel.oauth_id == google_id).first()
        
        if not user:
            # Check if email exists
            user = db.query(UserModel).filter(UserModel.email == email).first()
            if user:
                # Link OAuth to existing user
                user.oauth_id = google_id
                user.oauth_provider = "google"
            else:
                # Create new user
                user = UserModel(
                    username=email.split("@")[0],
                    email=email,
                    full_name=name,
                    oauth_id=google_id,
                    oauth_provider="google",
                    is_active=True
                )
                db.add(user)
        
        db.commit()
        db.refresh(user)
        
        access_token = create_access_token(data={"sub": user.id})
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "full_name": user.full_name
            }
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Google authentication failed: {str(e)}")

# ===================== FACEBOOK OAUTH2 =====================
@app.post("/api/auth/facebook")
async def facebook_auth(token_data: FacebookToken, db: Session = Depends(get_db)):
    """Authenticate with Facebook OAuth2 token"""
    try:
        # Verify token with Facebook
        facebook_url = "https://graph.facebook.com/me"
        response = requests.get(
            facebook_url,
            params={
                "access_token": token_data.token,
                "fields": "id,name,email,picture"
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Invalid Facebook token")
        
        fb_user = response.json()
        fb_id = fb_user.get("id")
        email = fb_user.get("email", f"facebook_{fb_id}@mixi.local")
        name = fb_user.get("name")
        
        # Find or create user
        user = db.query(UserModel).filter(UserModel.oauth_id == fb_id).first()
        
        if not user:
            # Check if email exists (only if email provided)
            if "@mixi.local" not in email:
                user = db.query(UserModel).filter(UserModel.email == email).first()
            
            if user:
                # Link OAuth to existing user
                user.oauth_id = fb_id
                user.oauth_provider = "facebook"
            else:
                # Create new user
                user = UserModel(
                    username=name.lower().replace(" ", "_") if name else f"fb_user_{fb_id}",
                    email=email,
                    full_name=name,
                    oauth_id=fb_id,
                    oauth_provider="facebook",
                    is_active=True
                )
                db.add(user)
        
        db.commit()
        db.refresh(user)
        
        access_token = create_access_token(data={"sub": user.id})
        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "full_name": user.full_name
            }
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Facebook authentication failed: {str(e)}")

# ===================== USER ENDPOINTS =====================
@app.get("/api/auth/me", response_model=UserResponse)
async def read_current_user(current_user: UserModel = Depends(get_current_user)):
    """Get current authenticated user info"""
    return current_user

@app.get("/api/auth/verify", response_model=dict)
async def verify_token(current_user: UserModel = Depends(get_current_user)):
    """Verify if token is valid"""
    return {"valid": True, "user_id": current_user.id}

# ===================== HEALTH CHECK =====================
@app.get("/api/auth/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok", "service": "Auth Service"}

@app.get("/health")
async def root_health():
    """Root health endpoint"""
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
