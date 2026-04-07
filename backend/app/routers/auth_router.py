from fastapi import APIRouter, Body
import hashlib

router = APIRouter(tags=["auth"])

@router.post("/login")
def login(payload: dict = Body(...)):
    email = payload.get("email", "")
    if not email:
        return {"user_id": ""}
    user_id = hashlib.md5(email.encode()).hexdigest()[:8]
    return {"user_id": user_id}
