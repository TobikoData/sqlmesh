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
import {
  ErrorType,
  ErrorTypeGeneric,
  ErrorTypeInvalidState,
  ErrorTypeSQLMeshOutdated,
} from '../utilities/errors'
import { CustomLSPMethods } from './custom'

type SupportedMethodsState =
  | { type: 'not-fetched' }
  | { type: 'fetched'; methods: Set<string> }
  // TODO: This state is used when the `sqlmesh/supported_methods` endpoint is
  //  not supported by the LSP server. This is in order to be backward compatible
  //  with older versions of SQLMesh that do not support this endpoint. At some point
  //  we should remove this state and always fetch the supported methods.
  | { type: 'endpoint-not-supported' }

let outputChannel: OutputChannel | undefined

export class LSPClient implements Disposable {
  private client: LanguageClient | undefined
  /**
   * State to track whether the supported methods have been fetched. These are used to determine if a method is supported
   * by the LSP server and return an error if not.
   */
  private supportedMethodsState: SupportedMethodsState = { type: 'not-fetched' }

  constructor() {
    this.client = undefined
  }

  // TODO: This method is used to check if the LSP client has completion capability
  //  in order to be backward compatible with older versions of SQLMesh that do not
  //  support completion. At some point we should remove this method and always assume
  //  that the LSP client has completion capability.
  public hasCompletionCapability(): boolean {
    if (!this.client) {
      traceError('LSP client is not initialized')
      return false
    }
    const capabilities = this.client.initializeResult?.capabilities
    const completion = capabilities?.completionProvider
    return completion !== undefined
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
          // TODO: This is a temporary fix to avoid the issue with the LSP server
          //  crashing when the number of workers is too high. This is a workaround
          //  to avoid the issue. Once fixed, we should remove the whole env block.
          env: {
            MAX_FORK_WORKERS: '1',
            ...process.env,
            ...sqlmesh.value.env,
          },
        },
        args: sqlmesh.value.args,
      },
      debug: {
        command: sqlmesh.value.bin,
        transport: TransportKind.stdio,
        options: {
          cwd: workspacePath,
          env: {
            // TODO: This is a temporary fix to avoid the issue with the LSP server
            //  crashing when the number of workers is too high. This is a workaround
            //  to avoid the issue. Once fixed, we should remove the whole env block.
            MAX_FORK_WORKERS: '1',
            ...process.env,
            ...sqlmesh.value.env,
          },
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
      // Reset supported methods state when the client stops
      this.supportedMethodsState = { type: 'not-fetched' }
    }
  }

  public async dispose() {
    await this.stop()
  }

  private async fetchSupportedMethods(): Promise<void> {
    if (!this.client || this.supportedMethodsState.type !== 'not-fetched') {
      return
    }
    try {
      const result = await this.internal_call_custom_method(
        'sqlmesh/supported_methods',
        {},
      )
      if (isErr(result)) {
        traceError(`Failed to fetch supported methods: ${result.error}`)
        this.supportedMethodsState = { type: 'endpoint-not-supported' }
        return
      }
      const methodNames = new Set(result.value.methods.map(m => m.name))
      this.supportedMethodsState = { type: 'fetched', methods: methodNames }
      traceInfo(
        `Fetched supported methods: ${Array.from(methodNames).join(', ')}`,
      )
    } catch {
      // If the supported_methods endpoint doesn't exist, mark it as not supported
      this.supportedMethodsState = { type: 'endpoint-not-supported' }
      traceInfo(
        'Supported methods endpoint not available, proceeding without validation',
      )
    }
  }

  public async call_custom_method<
    Method extends Exclude<
      CustomLSPMethods['method'],
      'sqlmesh/supported_methods'
    >,
    Request extends Extract<CustomLSPMethods, { method: Method }>['request'],
    Response extends Extract<CustomLSPMethods, { method: Method }>['response'],
  >(
    method: Method,
    request: Request,
  ): Promise<
    Result<
      Response,
      ErrorTypeGeneric | ErrorTypeInvalidState | ErrorTypeSQLMeshOutdated
    >
  > {
    if (!this.client) {
      return err({
        type: 'generic',
        message: 'LSP client not ready.',
      })
    }
    await this.fetchSupportedMethods()

    const supportedState = this.supportedMethodsState
    switch (supportedState.type) {
      case 'not-fetched':
        return err({
          type: 'invalid_state',
          message: 'Supported methods not fetched yet whereas they should.',
        })
      case 'fetched': {
        // If we have fetched the supported methods, we can check if the method is supported
        if (!supportedState.methods.has(method)) {
          return err({
            type: 'sqlmesh_outdated',
            message: `Method '${method}' is not supported by this LSP server.`,
          })
        }
        const response = await this.internal_call_custom_method(
          method,
          request as any,
        )
        if (isErr(response)) {
          return err({
            type: 'generic',
            message: response.error,
          })
        }
        return ok(response.value as Response)
      }
      case 'endpoint-not-supported': {
        const response = await this.internal_call_custom_method(
          method,
          request as any,
        )
        if (isErr(response)) {
          return err({
            type: 'generic',
            message: response.error,
          })
        }
        return ok(response.value as Response)
      }
    }
  }

  /**
   * Internal method to call a custom LSP method without checking if the method is supported. It is used for
   * the class whereas the `call_custom_method` checks if the method is supported.
   */
  public async internal_call_custom_method<
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
