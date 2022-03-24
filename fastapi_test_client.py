from dataclasses import dataclass, asdict

from fastapi import FastAPI, Depends
from starlette.testclient import TestClient

app = FastAPI()

@dataclass
class MyQueryParams:
    x: int
    y: str


@app.get('/renew_access_token')
def renew_access_token(qp: MyQueryParams = Depends(MyQueryParams)):
    return asdict(qp)


response = TestClient(app).get("/renew_access_token", params={"x": "1", "y": "2"})
print(response.json())
assert response.json() == {"x": 1, "y": "2"}