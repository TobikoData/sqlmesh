import { window, OutputChannel, Disposable } from "vscode"
import { ServerOptions, LanguageClientOptions, LanguageClient, TransportKind } from "vscode-languageclient/node"
import { sqlmesh_lsp_exec } from "../utilities/sqlmesh/sqlmesh"
import { err, isErr, ok, Result } from "../utilities/functional/result"
import { getWorkspaceFolders } from "../utilities/common/vscodeapi"
import { traceError } from "../utilities/common/log"

let outputChannel: OutputChannel | undefined

export class LSPClient implements Disposable {
    private client: LanguageClient | undefined

    constructor() {
        this.client = undefined
    }

    public async start(): Promise<Result<undefined, string>> {
        if (!outputChannel) {
            outputChannel = window.createOutputChannel('sqlmesh-lsp')
        }

        const sqlmesh = await sqlmesh_lsp_exec()
        if (isErr(sqlmesh)) {
            traceError(`Failed to get sqlmesh_lsp_exec: ${sqlmesh.error}`)
            return sqlmesh
        }
        const workspaceFolders = getWorkspaceFolders()
        if (workspaceFolders.length !== 1) {
            traceError(`Invalid number of workspace folders: ${workspaceFolders.length}`)
            return err("Invalid number of workspace folders")
        }
    
        let folder = workspaceFolders[0]
        const workspacePath = workspaceFolders[0].uri.fsPath
        let serverOptions: ServerOptions = {
            run: {
                command: sqlmesh.value.bin,
                transport: TransportKind.stdio,
                options: {
                    cwd: workspacePath,
                },
                args: sqlmesh.value.args,
            },
            debug: {
                command: sqlmesh.value.bin,
                transport: TransportKind.stdio,
                options: {
                    cwd: workspacePath,
                },
                args: sqlmesh.value.args,
            }
        }
        let clientOptions: LanguageClientOptions = {
            documentSelector: [
                { scheme: 'file', pattern: `**/*.sql` }
            ],
            workspaceFolder: folder,
            diagnosticCollectionName: 'sqlmesh',
            outputChannel: outputChannel,
            // synchronize: {
                // fileEvents: workspace.createFileSystemWatcher('**/*.{sql,py}'),
            // }
        }
    
        this.client = new LanguageClient('sqlmesh-lsp', 'SQLMesh Language Server', serverOptions, clientOptions)
        await this.client.start()
        return ok(undefined)
    }

    public async restart() {
        await this.stop()
        await this.start()
    }

    public async stop() {
        if (this.client) {
            await this.client.stop()
            this.client = undefined
        }
    }

    public async dispose() {
        await this.stop()
    }
}
