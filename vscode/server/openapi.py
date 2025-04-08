import json

from fastapi import FastAPI

from web.server.main import create_app


def generate_openapi_spec(app: FastAPI, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(app.openapi(), f)


if __name__ == "__main__":
    app = create_app()

    generate_openapi_spec(app, "web/client/openapi.json")
