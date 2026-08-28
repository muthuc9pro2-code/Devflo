"""Request-schema validation for credentials that create a new secret."""

import pytest
from pydantic import ValidationError

from app.schemas.user import ResetPasswordRequest, UserLogin, UserRegister

VALID_EMAIL = "user@example.com"


def _register(*, username: str = "devflo_user", password: str = "password"):
    return UserRegister(username=username, email=VALID_EMAIL, password=password)


@pytest.mark.parametrize(
    "username",
    [
        "abc",
        "a" * 30,
        "DevfloUser_123",
    ],
)
def test_registration_accepts_valid_username_boundaries_and_characters(username):
    request = _register(username=username)

    assert request.username == username


@pytest.mark.parametrize(
    "username",
    [
        "ab",
        "a" * 31,
        "muthu kumar",
        "muthu@123",
        "user-name",
        "",
        " leading",
        "trailing ",
    ],
)
def test_registration_rejects_invalid_usernames(username):
    with pytest.raises(ValidationError):
        _register(username=username)


@pytest.mark.parametrize("password", ["a" * 8, "a" * 128])
def test_registration_accepts_new_password_boundaries(password):
    request = _register(password=password)

    assert request.password == password


@pytest.mark.parametrize("password", ["a" * 7, "a" * 129])
def test_registration_rejects_new_password_outside_bounds(password):
    with pytest.raises(ValidationError):
        _register(password=password)


@pytest.mark.parametrize("new_password", ["a" * 8, "a" * 128])
def test_password_reset_accepts_new_password_boundaries(new_password):
    request = ResetPasswordRequest(token="reset-token", new_password=new_password)

    assert request.new_password == new_password


@pytest.mark.parametrize("new_password", ["a" * 7, "a" * 129])
def test_password_reset_rejects_new_password_outside_bounds(new_password):
    with pytest.raises(ValidationError):
        ResetPasswordRequest(token="reset-token", new_password=new_password)


def test_registration_preserves_existing_email_validation():
    request = UserRegister(
        username="valid_user",
        email=VALID_EMAIL,
        password="password",
    )

    assert request.email == VALID_EMAIL

    with pytest.raises(ValidationError):
        UserRegister(
            username="valid_user",
            email="not-an-email",
            password="password",
        )


@pytest.mark.parametrize("password", ["1", "x" * 128])
def test_login_password_accepts_bounded_values(password):
    request = UserLogin(email=VALID_EMAIL, password=password)

    assert request.password == password


@pytest.mark.parametrize("password", ["", "x" * 129])
def test_login_password_rejects_values_outside_bounds(password):
    with pytest.raises(ValidationError):
        UserLogin(email=VALID_EMAIL, password=password)


@pytest.mark.parametrize("token", ["x", "x" * 4096])
def test_password_reset_token_accepts_reasonable_jwt_bounds(token):
    request = ResetPasswordRequest(token=token, new_password="new-password")

    assert request.token == token


@pytest.mark.parametrize("token", ["", "x" * 4097])
def test_password_reset_token_rejects_values_outside_bounds(token):
    with pytest.raises(ValidationError):
        ResetPasswordRequest(token=token, new_password="new-password")
