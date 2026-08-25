from pathlib import Path
from tempfile import NamedTemporaryFile
from fastapi import APIRouter, File, HTTPException, UploadFile
from app.core.processing_config import MAX_OCR_IMAGE_BYTES, MEBIBYTE
from app.schemas.image import ImageInvestigationResponse
from app.services.image_text_extractor import (
    InvalidOcrImageError,
    OcrImageTooLargeError,
    extract_text_from_image,
    validate_ocr_image,
)
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

    content = await image.read(MAX_OCR_IMAGE_BYTES + 1)

    if len(content) > MAX_OCR_IMAGE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Image exceeds the {MAX_OCR_IMAGE_BYTES // MEBIBYTE} MiB limit",
        )

    suffix = Path(image.filename or "").suffix.lower()

    with NamedTemporaryFile(
        suffix=suffix,
        delete=True,
    ) as temp_file:
        temp_file.write(content)
        temp_file.flush()

        try:
            validate_ocr_image(temp_file.name)
        except OcrImageTooLargeError as error:
            raise HTTPException(status_code=413, detail=str(error)) from error
        except InvalidOcrImageError as error:
            raise HTTPException(status_code=415, detail=str(error)) from error

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