import json

from fastapi import FastAPI

from web.server.main import create_app


def generate_openapi_spec(app: FastAPI, path: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(app.openapi(), f)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate OpenAPI specification")
    parser.add_argument(
        "--output", default="web/client/openapi.json", help="Path to output OpenAPI spec file"
    )
    args = parser.parse_args()

    app = create_app()
    generate_openapi_spec(app, args.output)
