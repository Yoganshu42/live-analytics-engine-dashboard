from typing import Annotated
from pydantic import BaseModel, Field, EmailStr, StringConstraints

ZOPPER_EMAIL_REGEX = r"^[a-z]+\.[a-z]+@zopper\.com$"
ZOPPER_EMAIL = Annotated[str, StringConstraints(pattern=ZOPPER_EMAIL_REGEX)]


class LoginRequest(BaseModel):
    email: ZOPPER_EMAIL
    password: str
    role: str = Field(..., pattern="^(admin|employee)$")


class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    role: str
    email: EmailStr


class UserResponse(BaseModel):
    email: EmailStr
    role: str
    is_active: bool


class CreateUserRequest(BaseModel):
    email: ZOPPER_EMAIL
    password: str
    role: str = Field(..., pattern="^(admin|employee)$")
