from collections.abc import Iterable, Iterator


MAX_BATCH_BYTES = 5 * 1024 * 1024
MAX_BATCH_ITEMS = 20_000


def create_batches(
    items: Iterable[tuple[str, int]],
    max_batch_bytes: int = MAX_BATCH_BYTES,
    max_batch_items: int = MAX_BATCH_ITEMS,
) -> Iterator[list[tuple[str, int]]]:

    batch = []
    batch_bytes = 0

    for line, offset in items:
        line_bytes = len(line.encode("utf-8"))

        if batch and (
            batch_bytes + line_bytes > max_batch_bytes
            or len(batch) >= max_batch_items
        ):
            yield batch

            batch = []
            batch_bytes = 0

        batch.append((line, offset))
        batch_bytes += line_bytes

    if batch:
        yield batch