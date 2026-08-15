from pathlib import Path
from rapidocr_onnxruntime import RapidOCR
_ocr = RapidOCR()

_ALLOWED_IMAGE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
}

def extract_text_from_image(file_path: str) -> str:
    path = Path(file_path)

    if path.suffix.lower() not in _ALLOWED_IMAGE_EXTENSIONS:
        raise ValueError("Unsupported image format")

    results, _ = _ocr(str(path))

    if not results:
        return ""

    lines = [
        result[1].strip()
        for result in results
        if result[1].strip()
    ]

    return "\n".join(lines)