from fastapi import FastAPI

app = FastAPI(title="Behavioral Cohort Analysis API")


@app.get("/")
def read_root() -> dict[str, str]:
    return {"status": "ok"}
