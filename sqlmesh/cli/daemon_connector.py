import json
import typing as t
import uuid
from pathlib import Path

from sqlmesh.core.console import JanitorState, JanitorStateRenderer
from sqlmesh.lsp.cli_calls import (
    DaemonCommunicationModeTCP,
    DaemonCommunicationModeUnixSocket,
    LockFile,
    generate_lock_file,
    return_lock_file_path,
)
from sqlmesh.utils.pydantic import PydanticModel


class LSPCLICallRequest(PydanticModel):
    """Request to call a CLI command through the LSP."""

    arguments: t.List[str]


class SocketMessageFinished(PydanticModel):
    state: t.Literal["finished"] = "finished"


class SocketMessageOngoing(PydanticModel):
    state: t.Literal["ongoing"] = "ongoing"
    message: t.Dict[str, t.Any]


class SocketMessageError(PydanticModel):
    state: t.Literal["error"] = "error"
    message: str


SocketMessage = t.Union[SocketMessageFinished, SocketMessageOngoing, SocketMessageError]


def _validate_lock_file(lock_file_path: Path) -> LockFile:
    """Validate that the lock file is compatible with current version."""
    current_lock = generate_lock_file()
    try:
        read_file = LockFile.model_validate_json(lock_file_path.read_text())
    except Exception as e:
        raise ValueError(f"Failed to parse lock file: {e}")

    if not read_file.validate_lock_file(current_lock):
        raise ValueError(
            f"Lock file version mismatch. Expected: {current_lock.version}, "
            f"Got: {read_file.version}"
        )
    return read_file


import socket


class DaemonConnector:
    """Connects to the LSP daemon via socket to execute commands."""

    def __init__(self, project_path: Path, lock_file: LockFile):
        self.project_path = project_path
        self.lock_file = lock_file
        self.renderer = JanitorStateRenderer()

    def _open_connection(self) -> tuple[t.BinaryIO, t.BinaryIO]:
        lock_file = self.lock_file
        communication = lock_file.communication
        print(f"Using communication mode: {communication}")
        if communication is None:
            raise ValueError("not correct")

        if isinstance(communication.type, DaemonCommunicationModeUnixSocket):
            print("Opening Unix socket connection...")
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(communication.type.socket)
            print(f"Connected to Unix socket at {communication.type.socket}")
            rfile = sock.makefile("rb", buffering=0)
            wfile = sock.makefile("wb", buffering=0)
            print("Connected to daemon via Unix socket.")
            return rfile, wfile
        else:
            raise ValueError("Only Unix socket communication is supported")
        
    def _send_jsonrpc_request(self, connection: t.Any, method: str, params: dict) -> str:
        """Send a JSON-RPC request over the connection and return the request ID."""
        request_id = str(uuid.uuid4())
        jsonrpc_request = {"jsonrpc": "2.0", "method": method, "params": params, "id": request_id}

        # JSON-RPC over connection uses Content-Length header (LSP protocol style)
        message = json.dumps(jsonrpc_request)
        content_length = len(message.encode("utf-8"))

        # Send with Content-Length header
        header = f"Content-Length: {content_length}\r\n\r\n"
        full_message = header.encode("utf-8") + message.encode("utf-8")
        connection.write(full_message)
        connection.flush()

        return request_id

    def _read_jsonrpc_message(self, connection: t.Any) -> t.Dict[str, t.Any]:
        """Read any JSON-RPC message (response or notification) from the connection."""
        # Read headers
        headers = b""
        while b"\r\n\r\n" not in headers:
            chunk = connection.read(1)
            if not chunk:
                raise ValueError("Connection closed while reading headers")
            headers += chunk

        # Parse Content-Length header
        header_str = headers.decode("utf-8")
        content_length = None
        for line in header_str.split("\r\n"):
            if line.startswith("Content-Length:"):
                content_length = int(line.split(":")[1].strip())
                break

        if content_length is None:
            raise ValueError("No Content-Length header found")

        # Read the content
        content = connection.read(content_length)
        if len(content) < content_length:
            raise ValueError("Connection closed while reading content")

        # Parse JSON-RPC message
        message = json.loads(content.decode("utf-8"))
        return message

    def _read_jsonrpc_response(self, connection: t.Any, expected_id: str) -> t.Any:
        """Read a JSON-RPC response from the connection."""
        message = self._read_jsonrpc_message(connection)

        if message.get("id") != expected_id:
            raise ValueError(f"Unexpected response ID: {message.get('id')}")

        if "error" in message:
            raise ValueError(f"JSON-RPC error: {message['error']}")

        return message.get("result", {})

    def call_janitor(self, ignore_ttl: bool = False) -> bool:
        rfile = wfile = None
        try:
            rfile, wfile = self._open_connection()

            request = LSPCLICallRequest(
                arguments=["janitor"] + (["--ignore-ttl"] if ignore_ttl else [])
            )
            request_id = self._send_jsonrpc_request(wfile, "sqlmesh/cli/call", request.model_dump())

            with self.renderer as renderer:
                while True:
                    try:
                        message_data = self._read_jsonrpc_message(rfile)
                        if "id" in message_data and message_data["id"] == request_id:
                            result = message_data.get("result", {})
                            if result.get("state") == "finished":
                                return True
                            elif result.get("state") == "error":
                                print(f"Error from daemon: {result.get('message', 'Unknown error')}")
                                return False
                        elif message_data.get("method") == "sqlmesh/cli/update":
                            params = message_data.get("params", {})
                            if params.get("state") == "ongoing":
                                message = params.get("message", {})
                                if "state" in message:
                                    janitor_state = JanitorState.model_validate(message)
                                    renderer.render(janitor_state.state)
                    except Exception as stream_error:
                        print(f"Stream ended: {stream_error}")
                        break
            return True
        except Exception as e:
            print(f"Failed to communicate with daemon: {e}")
            return False
        finally:
            try:
                if rfile: rfile.close()
            finally:
                if wfile: wfile.close()

def get_daemon_connector(project_path: Path) -> t.Optional[DaemonConnector]:
    """Get a daemon connector if a valid lock file exists."""
    lock_path = return_lock_file_path(project_path)

    if not lock_path.exists():
        return None

    try:
        # Validate the lock file
        lock_file = _validate_lock_file(lock_path)

        # Check if communication info is present
        if lock_file.communication is None:
            return None

        return DaemonConnector(project_path, lock_file)
    except Exception as e:
        # Log the error but don't fail - fall back to direct execution
        print(f"Warning: Could not connect to daemon: {e}")
        return None
