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
import { resolveProjectPath } from '../utilities/config'

type SupportedMethodsState =
  | { type: 'not-fetched' }
  | { type: 'fetched'; methods: Set<string> }
  | { type: 'endpoint-not-supported' } // fallback for very old servers

let outputChannel: OutputChannel | undefined

export class LSPClient implements Disposable {
  private client: LanguageClient | undefined

  /** Caches which custom methods the server supports */
  private supportedMethodsState: SupportedMethodsState = { type: 'not-fetched' }

  /**
   * Remember whether the user explicitly stopped the client so that we do not
   * auto‑start again until they ask for it.
   */
  private explicitlyStopped = false

  /** True when a LanguageClient instance is alive. */
  private get isRunning(): boolean {
    return this.client !== undefined
  }

  /**
   * Query whether the connected server advertises completion capability.
   * (Transient helper kept for backwards‑compat reasons.)
   */
  public hasCompletionCapability(): boolean {
    if (!this.client) {
      traceError('LSP client is not initialized')
      return false
    }
    return (
      this.client.initializeResult?.capabilities?.completionProvider !==
      undefined
    )
  }

  /** Start the Language Client unless it is already running. */
  public async start(
    overrideStoppedByUser = false,
  ): Promise<Result<undefined, ErrorType>> {
    if (this.explicitlyStopped && !overrideStoppedByUser) {
      traceInfo(
        'LSP client has been explicitly stopped by user, not starting again.',
      )
      return ok(undefined)
    }

    // Guard against duplicate initialisation
    if (this.isRunning) {
      traceInfo('LSP client already running – start() is a no‑op.')
      return ok(undefined)
    }

    // Ensure we have an output channel
    if (!outputChannel) {
      outputChannel = window.createOutputChannel('sqlmesh-lsp')
    }

    // Resolve sqlmesh executable
    const sqlmesh = await sqlmeshLspExec()
    if (isErr(sqlmesh)) {
      traceError(
        `Failed to get sqlmesh_lsp_exec, ${JSON.stringify(sqlmesh.error)}`,
      )
      return sqlmesh
    }

    // We need at least one workspace
    if (getWorkspaceFolders().length === 0) {
      const msg = 'No workspace folders found'
      traceError(msg)
      return err({ type: 'generic', message: msg })
    }

    const workspacePath = sqlmesh.value.workspacePath
    const serverOptions: ServerOptions = {
      run: {
        command: sqlmesh.value.bin,
        transport: TransportKind.stdio,
        options: { cwd: workspacePath, env: sqlmesh.value.env },
        args: sqlmesh.value.args,
      },
      debug: {
        command: sqlmesh.value.bin,
        transport: TransportKind.stdio,
        options: { cwd: workspacePath, env: sqlmesh.value.env },
        args: sqlmesh.value.args,
      },
    }
    const paths = resolveProjectPath(getWorkspaceFolders()[0])
    if (isErr(paths)) {
      traceError(`Failed to resolve project paths: ${paths.error}`)
      return err({ type: 'generic', message: paths.error })
    }
    const clientOptions: LanguageClientOptions = {
      documentSelector: [
        { scheme: 'file', pattern: '**/*.sql' },
        { scheme: 'file', pattern: '**/external_models.yaml' },
        { scheme: 'file', pattern: '**/external_models.yml' },
      ],
      diagnosticCollectionName: 'sqlmesh',
      outputChannel,
      initializationOptions: paths.value.projectPaths
        ? {
            project_paths: paths.value.projectPaths,
          }
        : null,
    }

    traceInfo(
      `Starting SQLMesh LSP (cwd=${workspacePath})\n` +
        `  serverOptions=${JSON.stringify(serverOptions)}\n` +
        `  clientOptions=${JSON.stringify(clientOptions)}`,
    )

    this.client = new LanguageClient(
      'sqlmesh-lsp',
      'SQLMesh Language Server',
      serverOptions,
      clientOptions,
    )
    this.explicitlyStopped = false // user wanted it running again
    await this.client.start()
    return ok(undefined)
  }

  /** Restart = stop + start. */
  public async restart(
    overrideStoppedByUser = false,
  ): Promise<Result<undefined, ErrorType>> {
    await this.stop() // this also disposes
    return this.start(overrideStoppedByUser)
  }

  /**
   * Stop the client (if running) and clean up all VS Code resources so that a
   * future `start()` registers its commands without collisions.
   */
  public async stop(stoppedByUser = false): Promise<void> {
    if (this.client) {
      // Shut down the JSON‑RPC connection
      await this.client
        .stop()
        .catch(err => traceError(`Error while stopping LSP: ${err}`))

      // Unregister commands, code lenses, etc.
      await this.client.dispose()

      this.client = undefined
      this.supportedMethodsState = { type: 'not-fetched' }
      traceInfo('SQLMesh LSP client disposed.')
    }

    if (stoppedByUser) {
      this.explicitlyStopped = true
      traceInfo('SQLMesh LSP client stopped by user.')
    }
  }

  public async dispose(): Promise<void> {
    await this.stop()
  }

  private async fetchSupportedMethods(): Promise<void> {
    if (!this.client || this.supportedMethodsState.type !== 'not-fetched')
      return

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
    traceInfo(`Fetched supported methods: ${[...methodNames].join(', ')}`)
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
      return err({ type: 'generic', message: 'LSP client not ready.' })
    }

    await this.fetchSupportedMethods()

    const supportedState = this.supportedMethodsState
    if (
      supportedState.type === 'fetched' &&
      !supportedState.methods.has(method)
    ) {
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
      return err({ type: 'generic', message: response.error })
    }
    return ok(response.value as Response)
  }

  /**
   * Low‑level helper that sends a raw JSON‑RPC request without any feature checks.
   */
  public async internal_call_custom_method<
    Method extends CustomLSPMethods['method'],
    Request extends Extract<CustomLSPMethods, { method: Method }>['request'],
    Response extends Extract<CustomLSPMethods, { method: Method }>['response'],
  >(method: Method, request: Request): Promise<Result<Response, string>> {
    if (!this.client) return err('lsp client not ready')

    try {
      const result = await this.client.sendRequest<Response>(method, request)
      if ((result as any).response_error)
        return err((result as any).response_error)
      return ok(result)
    } catch (error) {
      traceError(`LSP '${method}' request failed: ${JSON.stringify(error)}`)
      return err(JSON.stringify(error))
    }
  }
}
