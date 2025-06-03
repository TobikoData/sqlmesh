import { window, OutputChannel, Disposable } from 'vscode'
import {
  ServerOptions,
  LanguageClientOptions,
  LanguageClient,
  TransportKind,
} from 'vscode-languageclient/node'
import { sqlmeshLspExec } from '../utilities/sqlmesh/sqlmesh'
import { err, isErr, ok, Result } from '@bus/result'
import { getWorkspaceFolders } from '../utilities/common/vscodeapi'
import { traceError, traceInfo } from '../utilities/common/log'
import { ErrorType } from '../utilities/errors'
import { CustomLSPMethods } from './custom'

let outputChannel: OutputChannel | undefined

export class LSPClient implements Disposable {
  private client: LanguageClient | undefined

  constructor() {
    this.client = undefined
  }

  public async start(): Promise<Result<undefined, ErrorType>> {
    if (!outputChannel) {
      outputChannel = window.createOutputChannel('sqlmesh-lsp')
    }

    const sqlmesh = await sqlmeshLspExec()
    if (isErr(sqlmesh)) {
      traceError(
        `Failed to get sqlmesh_lsp_exec, ${JSON.stringify(sqlmesh.error)}`,
      )
      return sqlmesh
    }
    const workspaceFolders = getWorkspaceFolders()
    if (workspaceFolders.length === 0) {
      traceError(`No workspace folders found`)
      return err({
        type: 'generic',
        message: 'No workspace folders found',
      })
    }
    const workspacePath = sqlmesh.value.workspacePath
    const serverOptions: ServerOptions = {
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
      },
    }
    const clientOptions: LanguageClientOptions = {
      documentSelector: [{ scheme: 'file', pattern: `**/*.sql` }],
      diagnosticCollectionName: 'sqlmesh',
      outputChannel: outputChannel,
    }

    traceInfo(
      `Starting SQLMesh Language Server with workspace path: ${workspacePath} with server options ${JSON.stringify(serverOptions)} and client options ${JSON.stringify(clientOptions)}`,
    )
    this.client = new LanguageClient(
      'sqlmesh-lsp',
      'SQLMesh Language Server',
      serverOptions,
      clientOptions,
    )
    await this.client.start()
    return ok(undefined)
  }

  public async restart(): Promise<Result<undefined, ErrorType>> {
    await this.stop()
    return await this.start()
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

  public async call_custom_method<
    Method extends CustomLSPMethods['method'],
    Request extends Extract<CustomLSPMethods, { method: Method }>['request'],
    Response extends Extract<CustomLSPMethods, { method: Method }>['response'],
  >(method: Method, request: Request): Promise<Result<Response, string>> {
    if (!this.client) {
      return err('lsp client not ready')
    }
    try {
      const result = await this.client.sendRequest<Response>(method, request)
      return ok(result)
    } catch (error) {
      traceError(
        `lsp '${method}' request ${JSON.stringify(request)} failed: ${JSON.stringify(error)}`,
      )
      return err(JSON.stringify(error))
    }
  }
}
