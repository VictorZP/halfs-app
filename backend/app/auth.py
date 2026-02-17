"""JWT authentication for the Web API.

Uses a single admin account whose credentials are stored in environment
variables (``AUTH_USERNAME`` / ``AUTH_PASSWORD``).

Usage in a router::

    from backend.app.auth import require_auth
    @router.get("/protected", dependencies=[Depends(require_auth)])
    def secret():
        return {"msg": "ok"}
"""

import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

# ---------------------------------------------------------------------------
# Configuration (from environment)
# ---------------------------------------------------------------------------

JWT_SECRET: str = os.getenv("JWT_SECRET", "change-me-in-production-please")
JWT_ALGORITHM: str = "HS256"
JWT_EXPIRE_HOURS: int = int(os.getenv("JWT_EXPIRE_HOURS", "720"))  # 30 days

AUTH_USERNAME: str = os.getenv("AUTH_USERNAME", "admin")
AUTH_PASSWORD: str = os.getenv("AUTH_PASSWORD", "admin")

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"

# ---------------------------------------------------------------------------
# OAuth2 bearer scheme (extracts token from Authorization header)
# ---------------------------------------------------------------------------

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def verify_password(plain: str) -> bool:
    """Timing-safe comparison of the plain password against the stored one."""
    return secrets.compare_digest(plain, AUTH_PASSWORD)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(hours=JWT_EXPIRE_HOURS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


def authenticate_user(username: str, password: str) -> bool:
    """Return *True* if credentials match the admin account."""
    if not secrets.compare_digest(username, AUTH_USERNAME):
        return False
    return verify_password(password)

# ---------------------------------------------------------------------------
# FastAPI dependency — inject into any route to require authentication
# ---------------------------------------------------------------------------


async def require_auth(token: str = Depends(oauth2_scheme)) -> str:
    """Validate the JWT token and return the username.

    Raise ``401`` if the token is missing, expired, or invalid.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        username: Optional[str] = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username
