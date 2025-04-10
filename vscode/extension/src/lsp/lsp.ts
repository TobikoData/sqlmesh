import path from "path";
import { workspace, ExtensionContext } from "vscode";
import { ServerOptions, LanguageClientOptions, LanguageClient, TransportKind } from "vscode-languageclient/node";
import { sqlmesh_exec, sqlmesh_lsp_exec } from "../sqlmesh/sqlmesh";
import { err, isErr, ok, Result } from "../functional/result";
import { getWorkspaceFolders } from "../common/vscodeapi";

let client: LanguageClient;

export async function activateLsp(context: ExtensionContext): Promise<Result<undefined, string>> {
    const sqlmesh = await sqlmesh_lsp_exec()
    if (isErr(sqlmesh)) {
        return sqlmesh
    }
    const workspaceFolders = getWorkspaceFolders()
    if (workspaceFolders.length !== 1) {
        return err("Invalid number of workspace folders")
    }
    const workspacePath = workspaceFolders[0].uri.fsPath
    let serverOptions: ServerOptions = {
        run: {
            command: sqlmesh.value.bin,
            transport: TransportKind.stdio,
            options: {
                env: {
                    ...process.env,
                },
                cwd: workspacePath,
            }
        },
        debug: {
            command: sqlmesh.value.bin,
            transport: TransportKind.stdio,
            options: {
                env: {
                    ...process.env,
                },
                cwd: workspacePath,
            }
        }
    }
    let clientOptions: LanguageClientOptions = {
        documentSelector: [
            {
                scheme: 'file',
                language: 'sql',
                pattern: '**/*.sql'
            }
        ],
        synchronize: {
            fileEvents: workspace.createFileSystemWatcher('**/*.{sql,py}'),
        }
    }

    client = new LanguageClient('sqlmesh', 'SQLMesh Language Server', serverOptions, clientOptions)
    console.log('Starting language client')
    await client.start()
    console.log('Language client started')
    return ok(undefined)
}

export async function deactivateLsp() {
    if (client) {
        await client.stop()
    }
}
    