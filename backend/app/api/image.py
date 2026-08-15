from pathlib import Path
from tempfile import NamedTemporaryFile
from fastapi import APIRouter, File, HTTPException, UploadFile
from app.schemas.image import ImageInvestigationResponse
from app.services.image_text_extractor import extract_text_from_image
from app.services.ocr_normalizer import normalize_ocr_text

router = APIRouter(
    prefix="/image",
    tags=["Image Investigation"],
)

_ALLOWED_CONTENT_TYPES = {
    "image/png",
    "image/jpeg",
    "image/webp",
}

_MAX_IMAGE_SIZE = 10 * 1024 * 1024


@router.post(
    "/extract-text",
    response_model=ImageInvestigationResponse,
)
async def extract_image_text(
    image: UploadFile = File(...),
):
    if image.content_type not in _ALLOWED_CONTENT_TYPES:
        raise HTTPException(
            status_code=415,
            detail="Unsupported image format",
        )

    content = await image.read(_MAX_IMAGE_SIZE + 1)

    if len(content) > _MAX_IMAGE_SIZE:
        raise HTTPException(
            status_code=413,
            detail="Image exceeds 10 MB limit",
        )

    suffix = Path(image.filename or "").suffix.lower()

    with NamedTemporaryFile(
        suffix=suffix,
        delete=True,
    ) as temp_file:
        temp_file.write(content)
        temp_file.flush()

        extracted_text = extract_text_from_image(
            temp_file.name
        )
        extracted_text = normalize_ocr_text(extracted_text)

    if not extracted_text.strip():
        raise HTTPException(
            status_code=422,
            detail="No readable text found in image",
        )

    return ImageInvestigationResponse(
        extracted_text=extracted_text,
    )