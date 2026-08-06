from pydantic import BaseModel, EmailStr, ConfigDict

class UserRegister(BaseModel):
    username: str
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: int
    username: str
    email: EmailStr
    is_verified: bool

    model_config = ConfigDict(from_attributes=True)

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str

class RegisterResponse(BaseModel):
    message: str
    email: EmailStr
    






