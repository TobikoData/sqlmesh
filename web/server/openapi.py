import json

from web.server.main import app


def generate_openapi_spec(path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(app.openapi(), f)


if __name__ == "__main__":
    generate_openapi_spec("web/client/openapi.json")
