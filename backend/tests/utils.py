from __future__ import annotations

import io
from typing import Any

from fastapi.testclient import TestClient


def csv_upload(
    client: TestClient,
    *,
    csv_text: str,
    filename: str = "events.csv",
    content_type: str = "text/csv",
) -> Any:
    return client.post(
        "/upload",
        files={"file": (filename, io.BytesIO(csv_text.encode("utf-8")), content_type)},
    )
