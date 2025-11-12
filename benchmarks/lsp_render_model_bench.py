#!/usr/bin/env python

import asyncio
import pyperf
import os
import logging
from pathlib import Path
from lsprotocol import types

from sqlmesh.lsp.custom import RenderModelRequest, RENDER_MODEL_FEATURE
from sqlmesh.lsp.uri import URI
from pygls.client import JsonRPCClient

# Suppress debug logging during benchmark
logging.getLogger().setLevel(logging.WARNING)


class LSPClient(JsonRPCClient):
    """A custom LSP client for benchmarking."""
    
    def __init__(self):
        super().__init__()
        self.render_model_result = None
        self.initialized = asyncio.Event()
        
        # Register handlers for notifications we expect from the server
        @self.feature(types.WINDOW_SHOW_MESSAGE)
        def handle_show_message(_):
            # Silently ignore show message notifications during benchmark
            pass
        
        @self.feature(types.WINDOW_LOG_MESSAGE)
        def handle_log_message(_):
            # Silently ignore log message notifications during benchmark
            pass
        
    async def initialize_server(self):
        """Send initialization request to server."""
        # Get the sushi example directory
        sushi_dir = Path(__file__).parent.parent / "examples" / "sushi"
        
        response = await self.protocol.send_request_async(
            types.INITIALIZE,
            types.InitializeParams(
                process_id=os.getpid(),
                root_uri=URI.from_path(sushi_dir).value,
                capabilities=types.ClientCapabilities(),
                workspace_folders=[
                    types.WorkspaceFolder(
                        uri=URI.from_path(sushi_dir).value,
                        name="sushi"
                    )
                ]
            )
        )
        
        # Send initialized notification
        self.protocol.notify(types.INITIALIZED, types.InitializedParams())
        self.initialized.set()
        return response


async def benchmark_render_model_async(client: LSPClient, model_path: Path):
    """Benchmark the render_model request."""
    uri = URI.from_path(model_path).value
    
    # Send render_model request
    result = await client.protocol.send_request_async(
        RENDER_MODEL_FEATURE,
        RenderModelRequest(textDocumentUri=uri)
    )
    
    return result


def benchmark_render_model(loops):
    """Synchronous wrapper for the benchmark."""
    async def run():
        # Create client
        client = LSPClient()
        
        # Start the SQLMesh LSP server as a subprocess
        await client.start_io("python", "-m", "sqlmesh.lsp.main")
        
        # Initialize the server
        await client.initialize_server()
        
        # Get a model file to test with
        sushi_dir = Path(__file__).parent.parent / "examples" / "sushi"
        model_path = sushi_dir / "models" / "customers.sql"
        
        # Warm up
        await benchmark_render_model_async(client, model_path)
        
        # Run benchmark
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            await benchmark_render_model_async(client, model_path)
        dt = pyperf.perf_counter() - t0
        
        # Clean up
        await client.stop()
        
        return dt
    
    return asyncio.run(run())


def main():
    runner = pyperf.Runner()
    runner.bench_time_func(
        "lsp_render_model",
        benchmark_render_model
    )


if __name__ == "__main__":
    main()