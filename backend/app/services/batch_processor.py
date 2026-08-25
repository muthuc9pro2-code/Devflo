from collections.abc import Iterable, Iterator
from app.core.processing_config import (
    INGESTION_RAW_BATCH_BYTES,
    INGESTION_RAW_BATCH_ITEMS,
)

MAX_BATCH_BYTES = INGESTION_RAW_BATCH_BYTES
MAX_BATCH_ITEMS = INGESTION_RAW_BATCH_ITEMS


def _item_size_bytes(item: object) -> int:
    configured_size = getattr(item, "batch_size_bytes", None)

    if isinstance(configured_size, int):
        return configured_size

    if isinstance(item, tuple) and item and isinstance(item[0], str):
        return len(item[0].encode("utf-8"))

    raise TypeError("Batch items must expose batch_size_bytes or start with text")


def create_batches[BatchItem](
    items: Iterable[BatchItem],
    max_batch_bytes: int = MAX_BATCH_BYTES,
    max_batch_items: int = MAX_BATCH_ITEMS,
) -> Iterator[list[BatchItem]]:
    batch = []
    batch_bytes = 0

    for item in items:
        item_bytes = _item_size_bytes(item)

        if batch and (
            batch_bytes + item_bytes > max_batch_bytes or len(batch) >= max_batch_items
        ):
            yield batch

            batch = []
            batch_bytes = 0

        batch.append(item)
        batch_bytes += item_bytes

    if batch:
        yield batch
