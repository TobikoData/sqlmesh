import { ChildProcess, spawn } from "child_process"
import { isErr, ok, Result } from "../functional/result"
import getPort from "../get-port"
import { sqlmesh_exec } from "../sqlmesh/sqlmesh"
import * as vscode from 'vscode'

export interface Server {
    port: number;
    url: string;
    stop: () => void;
}

export const startWebServer = async (outputChannel: vscode.OutputChannel): Promise<Result<WebServer, string>> => {
    const sqlmesh = await sqlmesh_exec()
    if (isErr(sqlmesh)) {
        return sqlmesh
    }
    const port = await getPort()
    const { bin, workspacePath, env } = sqlmesh.value
    const process = spawn(bin, ['ui', '--port', port.toString()], {
        cwd: workspacePath,
        env,
    })
    
        process.stdout.on('data', (data) => {
            outputChannel.append(data.toString())
        })
        process.stderr.on('data', (data) => {
            outputChannel.append(data.toString())
        })
    
    return ok(new WebServer(port, `http://localhost:${port}`, process))
}

export class WebServer implements Server {
    port: number
    url: string
    process: ChildProcess

    constructor(port: number, url: string, process: ChildProcess) {
        this.port = port
        this.url = url
        this.process = process
    }

    stop() {
        this.process.kill()
    }
}

