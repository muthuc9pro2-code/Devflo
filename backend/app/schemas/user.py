from typing import Annotated

from pydantic import BaseModel, ConfigDict, EmailStr, StringConstraints

RegistrationUsername = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=30,
        pattern=r"^[A-Za-z0-9_]+$",
    ),
]
NewPassword = Annotated[str, StringConstraints(min_length=8, max_length=128)]


class UserRegister(BaseModel):
    username: RegistrationUsername
    email: EmailStr
    password: NewPassword


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
    message: str


class RegisterResponse(BaseModel):
    message: str
    email: EmailStr


class ForgotPasswordRequest(BaseModel):
    email: EmailStr


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: NewPassword
