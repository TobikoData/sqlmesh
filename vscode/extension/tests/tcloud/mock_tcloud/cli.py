"""
Mock tcloud CLI for testing VSCode extension.
Implements only the commands used by the extension.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import click

def get_auth_state_file():
    """Get the path to the auth state file in the current working directory"""
    return Path.cwd() / ".tcloud_auth_state.json"


def get_version_state_file():
    """Get the path to the version state file in the current working directory"""
    return Path.cwd() / ".tcloud_version_state.json"


def load_auth_state():
    """Load authentication state from file"""
    auth_file = get_auth_state_file()
    if auth_file.exists():
        with open(auth_file, "r") as f:
            return json.load(f)
    return {"is_logged_in": False, "id_token": None}


def save_auth_state(state):
    """Save authentication state to file"""
    auth_file = get_auth_state_file()
    with open(auth_file, "w") as f:
        json.dump(state, f)


def load_version_state():
    """Load version state from file"""
    version_file = get_version_state_file()
    if version_file.exists():
        with open(version_file, "r") as f:
            return json.load(f)
    # Default to version 2.10.0 if no state file exists
    return {"version": "2.10.0"}


@click.group(no_args_is_help=True, invoke_without_command=True)
@click.option(
    "--project",
    type=str,
    help="The name of the project.",
)
@click.option(
    "--version",
    is_flag=True,
    help="Show version",
)
@click.pass_context
def cli(ctx: click.Context, project: str, version: bool) -> None:
    """Mock Tobiko Cloud CLI"""
    if version:
        version_state = load_version_state()
        print(version_state["version"])
        ctx.exit(0)
    
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        ctx.exit(0)
    
    ctx.ensure_object(dict)
    ctx.obj["project"] = project


@cli.command("is_sqlmesh_installed", hidden=True)
@click.pass_context
def is_sqlmesh_installed(ctx: click.Context) -> None:
    """Check if SQLMesh Enterprise is installed"""
    # For testing, we'll track installation state in a file in the current bin directory
    # This matches where the test expects it to be
    bin_dir = Path(sys.executable).parent
    install_state_file = bin_dir / ".sqlmesh_installed"
    is_installed = install_state_file.exists()

    print(
        json.dumps(
            {
                "is_installed": is_installed,
            }
        )
    )


@cli.command("install_sqlmesh")
@click.pass_context
def install_sqlmesh(ctx: click.Context) -> None:
    """Install the correct version of SQLMesh Enterprise"""

    # For 3 seconds output to stdout
    for i in range(3):
        print(f"Installing SQLMesh Enterprise logs {i + 1}/3", flush=True)
        time.sleep(1)

    # Simulate installation by creating a marker file in the bin directory
    bin_dir = Path(sys.executable).parent
    install_state_file = bin_dir / ".sqlmesh_installed"
    install_state_file.touch()

    print("Mock SQLMesh Enterprise installed successfully")


@cli.command("sqlmesh")
@click.argument("args", nargs=-1)
@click.pass_context
def sqlmesh(ctx: click.Context, args) -> None:
    """Run SQLMesh Enterprise commands"""
    # Pass through to the real sqlmesh command

    # Get the path to sqlmesh in the same environment as this script
    bin_dir = os.path.dirname(sys.executable)
    sqlmesh_path = os.path.join(bin_dir, "sqlmesh")

    if not os.path.exists(sqlmesh_path):
        # Try with .exe extension on Windows
        sqlmesh_path = os.path.join(bin_dir, "sqlmesh.exe")

    if not os.path.exists(sqlmesh_path):
        # Fall back to using sqlmesh from PATH
        sqlmesh_path = "sqlmesh"

    # Execute the real sqlmesh with the provided arguments
    result = subprocess.run([sqlmesh_path] + list(args), capture_output=False)
    sys.exit(result.returncode)


@cli.command("sqlmesh_lsp")
@click.argument("args", nargs=-1)
@click.pass_context
def sqlmesh_lsp(ctx: click.Context, args) -> None:
    """Run SQLMesh LSP server"""
    # For testing purposes, we'll simulate the LSP server starting
    print("Starting SQLMesh LSP server...", flush=True)
    
    # Get the path to sqlmesh in the same environment as this script
    bin_dir = os.path.dirname(sys.executable)
    sqlmesh_path = os.path.join(bin_dir, "sqlmesh")

    if not os.path.exists(sqlmesh_path):
        # Try with .exe extension on Windows
        sqlmesh_path = os.path.join(bin_dir, "sqlmesh.exe")

    if not os.path.exists(sqlmesh_path):
        # Fall back to using sqlmesh from PATH
        sqlmesh_path = "sqlmesh"

    # Execute the real sqlmesh with lsp command and provided arguments
    result = subprocess.run([sqlmesh_path, "lsp"] + list(args), capture_output=False)
    sys.exit(result.returncode)


@click.group()
def auth() -> None:
    """
    Tobiko Cloud Authentication
    """


@auth.command()
def logout() -> None:
    """Logout of any current session"""
    save_auth_state({"is_logged_in": False, "id_token": None})
    print("Logged out successfully")


### Methods for VSCode
@auth.group(hidden=True)
def vscode() -> None:
    """Commands for VSCode integration"""
    pass


@vscode.command("login-url")
def login_url() -> None:
    """
    Login to Tobiko Cloud.

    This returns a JSON object with the following fields:
    - url: The URL to login open
    """
    # Return mock OAuth URL and verifier
    print(
        json.dumps(
            {
                "url": "https://mock-auth.example.com/auth?client_id=mock&redirect_uri=http://localhost:7890",
                "verifier_code": "mock_verifier_12345",
            }
        )
    )


@vscode.command("start-server")
@click.argument("code_verifier", type=str, required=True)
def start_server(code_verifier: str) -> None:
    """
    Start the server to catch the redirect from the browser.
    """
    # Simulate successful authentication after a short delay
    time.sleep(0.5)

    # Update auth state to logged in
    save_auth_state(
        {
            "is_logged_in": True,
            "id_token": {
                "iss": "https://mock.tobikodata.com",
                "aud": "mock-audience",
                "sub": "user-123",
                "scope": "openid email profile",
                "iat": int(time.time()),
                "exp": int(time.time()) + 3600,  # Token expires in 1 hour
                "email": "test@example.com",
                "name": "Test User",
            },
        }
    )

    # The real command would start a server, but for testing we just simulate success
    print("Mock server started successfully")


@vscode.command("status")
def vscode_status() -> None:
    """
    Auth status for logged in
    """
    state = load_auth_state()
    print(
        json.dumps(
            {"is_logged_in": state["is_logged_in"], "id_token": state["id_token"]}
        )
    )


@vscode.command("device")
def vscode_device() -> None:
    """
    Initiate device flow for VSCode integration
    """
    print(
        json.dumps(
            {
                "device_code": "MOCK-DEVICE-CODE",
                "user_code": "ABCD-1234",
                "verification_uri": "https://mock-auth.example.com/device",
                "verification_uri_complete": "https://mock-auth.example.com/device?user_code=ABCD-1234",
                "expires_in": 600,
                "interval": 5,
            }
        )
    )


@vscode.command("poll_device")
@click.argument("device_code", type=str, required=True)
@click.option(
    "-i",
    "--interval",
    type=int,
    default=5,
    help="The interval between polling attempts in seconds",
)
@click.option(
    "-t",
    "--timeout",
    type=int,
    default=300,
    help="The timeout for the device flow in seconds",
)
def vscode_poll_device(device_code: str, interval: int, timeout: int) -> None:
    """
    Poll the device flow for VSCode integration
    """
    # For testing, we'll just succeed immediately
    save_auth_state(
        {
            "is_logged_in": True,
            "id_token": {
                "iss": "https://mock.tobikodata.com",
                "aud": "mock-audience",
                "sub": "device-user-123",
                "scope": "openid email profile",
                "iat": int(time.time()),
                "exp": int(time.time()) + 3600,
                "email": "device@example.com",
                "name": "Device User",
            },
        }
    )

    print(json.dumps({"success": True}))


# Add auth group to main CLI
cli.add_command(auth)


if __name__ == "__main__":
    cli()
