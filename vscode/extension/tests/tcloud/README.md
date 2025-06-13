# Mock tcloud CLI for Testing

This directory contains a mock implementation of the tcloud CLI for testing the VSCode extension.

## Implemented Commands

The mock implements only the commands used by the VSCode extension:

### Authentication Commands
- `tcloud auth vscode status` - Returns authentication status
- `tcloud auth vscode login-url` - Returns mock OAuth login URL
- `tcloud auth vscode start-server <verifier_code>` - Simulates OAuth callback
- `tcloud auth vscode device` - Returns mock device flow info
- `tcloud auth vscode poll_device <device_code>` - Simulates device flow success
- `tcloud auth logout` - Clears authentication state

### SQLMesh Commands
- `tcloud is_sqlmesh_installed` - Checks installation status
- `tcloud install_sqlmesh` - Marks SQLMesh as installed
- `tcloud sqlmesh <args>` - Echoes sqlmesh commands

## State Management

The mock maintains state in two files:
- `.tcloud_auth_state.json` - Authentication state (logged in/out, ID token)
- `.sqlmesh_installed` - SQLMesh installation marker

## Usage in Tests

To use this mock in tests:

1. Ensure the mock is in PATH or reference it directly
2. The mock will simulate successful authentication flows
3. State persists between calls for realistic testing

## Example

```bash
# Check auth status
./tcloud auth vscode status
# Output: {"is_logged_in": false, "id_token": null}

# Simulate login
./tcloud auth vscode login-url
# Output: {"url": "https://mock-auth.example.com/auth?client_id=mock&redirect_uri=http://localhost:7890", "verifier_code": "mock_verifier_12345"}

./tcloud auth vscode start-server mock_verifier_12345
# Output: Mock server started successfully

# Check status again
./tcloud auth vscode status
# Output: {"is_logged_in": true, "id_token": {"email": "test@example.com", "name": "Test User", "exp": 1736790123}}
```