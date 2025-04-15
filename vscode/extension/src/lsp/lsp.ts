import { window, OutputChannel, Disposable } from "vscode"
import { ServerOptions, LanguageClientOptions, LanguageClient, TransportKind } from "vscode-languageclient/node"
import { sqlmesh_lsp_exec } from "../utilities/sqlmesh/sqlmesh"
import { err, isErr, ok, Result } from "../utilities/functional/result"
import { getWorkspaceFolders } from "../utilities/common/vscodeapi"

let outputChannel: OutputChannel | undefined

export class LSPClient implements Disposable {
    private client: LanguageClient | undefined

    constructor() {
        this.client = undefined
    }

    public async start(): Promise<Result<undefined, string>> {
        if (!outputChannel) {
            outputChannel = window.createOutputChannel('sqlmesh_actual_lsp_implementation')
        }

        const sqlmesh = await sqlmesh_lsp_exec()
        if (isErr(sqlmesh)) {
            return sqlmesh
        }
        const workspaceFolders = getWorkspaceFolders()
        if (workspaceFolders.length !== 1) {
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
            },
            debug: {
                command: sqlmesh.value.bin,
                transport: TransportKind.stdio,
                options: {
                    cwd: workspacePath,
                }
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
    
        this.client = new LanguageClient('sqlmesh-lsp-example', 'SQLMesh Language Server', serverOptions, clientOptions)
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
