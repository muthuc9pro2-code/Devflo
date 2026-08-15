import boto3
from app.core.config import Settings

ses_client = boto3.client(
    "ses",
    region_name=Settings.AWS_REGION,
    aws_access_key_id=Settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=Settings.AWS_SECRET_ACCESS_KEY,
)

def send_ses_email(to_email: str, verification_url: str) -> None:
    ses_client.send_email(
        Source=Settings.SES_FROM_EMAIL,
        Destination={
            "ToAddresses": [to_email],
        },
        Message={
            "Subject": {
                "Data": "Verify your Devflo account",
                "Charset": "UTF-8",
            },
            "Body": {
                "Text": {
                    "Data": (
                        "Welcome to Devflo.\n\n"
                        "Verify your email address using this link:\n"
                        f"{verification_url}\n\n"
                        "If you did not create this account, ignore this email."
                    ),
                    "Charset": "UTF-8",
                },
            },
        },
    )