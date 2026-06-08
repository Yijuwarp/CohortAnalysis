import io
from typing import Any
from fastapi.testclient import TestClient
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

DETERMINISTIC_USER_ID = "84176fc0"

class DeterministicTestClient(TestClient):
    """
    A TestClient wrapper that automatically injects a valid user_id 
    into all requests to pass back-end validation.
    """
    def request(self, method: str, url: str, **kwargs: Any) -> Any:
        # Check URL query string
        parsed = urlparse(url)
        url_params = dict(parse_qsl(parsed.query))
        
        # Check params kwarg (common in usage like client.get(url, params={...}))
        kw_params = kwargs.get("params")
        if kw_params is None:
            kw_params = {}
            
        # We only inject if user_id is missing from BOTH
        if "user_id" not in url_params and "user_id" not in kw_params:
            if kw_params:
                kw_params["user_id"] = DETERMINISTIC_USER_ID
                kwargs["params"] = kw_params
            else:
                url_params["user_id"] = DETERMINISTIC_USER_ID
                new_query = urlencode(url_params)
                url = urlunparse(parsed._replace(query=new_query))
                
        return super().request(method, url, **kwargs)


def csv_upload(
    client: TestClient,
    *,
    csv_text: str,
    filename: str = "events.csv",
    content_type: str = "text/csv",
    user_id: str | None = None,
) -> Any:
    """
    Uploads a CSV file using the provided client. 
    If the client is a DeterministicTestClient, user_id is handled automatically.
    """
    final_user_id = user_id or DETERMINISTIC_USER_ID
    # We use a path that might or might not have user_id, 
    # but the client wrapper will handle the injection if missing.
    return client.post(
        f"/upload?user_id={final_user_id}",
        files={"file": (filename, io.BytesIO(csv_text.encode("utf-8")), content_type)},
    )

