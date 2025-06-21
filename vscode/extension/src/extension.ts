import { format } from './commands/format'
import * as vscode from 'vscode'
import {
  createOutputChannel,
  onDidChangeConfiguration,
  registerCommand,
} from './utilities/common/vscodeapi'
import { registerLogger, traceInfo, traceVerbose } from './utilities/common/log'
import { onDidChangePythonInterpreter } from './utilities/common/python'
import { LSPClient } from './lsp/lsp'
import { AuthenticationProviderTobikoCloud } from './auth/auth'
import { signOut } from './commands/signout'
import { signIn } from './commands/signin'
import { signInSpecifyFlow } from './commands/signinSpecifyFlow'
import { renderModel, reRenderModelForSourceFile } from './commands/renderModel'
import { stop } from './commands/stop'
import { isErr } from '@bus/result'
import { handleError } from './utilities/errors'
import { selector, completionProvider } from './completion/completion'
import { LineagePanel } from './webviews/lineagePanel'
import { RenderedModelProvider } from './providers/renderedModelProvider'
import { sleep } from './utilities/sleep'

let lspClient: LSPClient | undefined

// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export async function activate(context: vscode.ExtensionContext) {
  const extensionOutputChannel = createOutputChannel('sqlmesh')
  context.subscriptions.push(
    extensionOutputChannel,
    registerLogger(extensionOutputChannel),
  )
  traceInfo('Activating SQLMesh extension')

  traceInfo('Registering authentication provider')
  const authProvider = new AuthenticationProviderTobikoCloud()
  context.subscriptions.push(
    vscode.authentication.registerAuthenticationProvider(
      AuthenticationProviderTobikoCloud.id,
      AuthenticationProviderTobikoCloud.name,
      authProvider,
      { supportsMultipleAccounts: false },
    ),
  )
  traceInfo('Authentication provider registered')

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.signin',
      signIn(authProvider, async () => {
        traceInfo('Restarting LSP after sign-in')
        await restart()
      }),
    ),
  )
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.signinSpecifyFlow',
      signInSpecifyFlow(authProvider, async () => {
        traceInfo('Restarting LSP after sign-in')
        await restart()
      }),
    ),
  )
  context.subscriptions.push(
    vscode.commands.registerCommand('sqlmesh.signout', signOut(authProvider)),
  )

  lspClient = new LSPClient()

  // Create and register the rendered model provider
  const renderedModelProvider = new RenderedModelProvider()
  context.subscriptions.push(
    vscode.workspace.registerTextDocumentContentProvider(
      RenderedModelProvider.getScheme(),
      renderedModelProvider,
    ),
    renderedModelProvider,
  )

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.renderModel',
      renderModel(lspClient, renderedModelProvider),
    ),
  )

  // Register the webview
  const lineagePanel = new LineagePanel(context.extensionUri, lspClient)
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(
      LineagePanel.viewType,
      lineagePanel,
    ),
  )

  // Add file save listener for auto-rerendering models
  context.subscriptions.push(
    vscode.workspace.onDidSaveTextDocument(async document => {
      if (
        lspClient &&
        renderedModelProvider.hasRenderedModelForSource(
          document.uri.toString(true),
        )
      ) {
        await sleep(100)
        await reRenderModelForSourceFile(
          document.uri.toString(true),
          lspClient,
          renderedModelProvider,
        )
      }
    }),
  )

  const restart = async () => {
    if (lspClient) {
      traceVerbose('Restarting LSP client')
      const restartResult = await lspClient.restart()
      if (isErr(restartResult)) {
        return handleError(
          authProvider,
          restart,
          restartResult.error,
          'LSP restart failed',
        )
      }
      context.subscriptions.push(lspClient)
    } else {
      lspClient = new LSPClient()
      const result = await lspClient.start()
      if (isErr(result)) {
        return handleError(
          authProvider,
          restart,
          result.error,
          'Failed to start LSP',
        )
      } else {
        context.subscriptions.push(lspClient)
      }
    }
  }

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.format',
      format(authProvider, lspClient, restart),
    ),
  )

  context.subscriptions.push(
    onDidChangePythonInterpreter(async () => {
      await restart()
    }),
    onDidChangeConfiguration(async () => {
      await restart()
    }),
    registerCommand(`sqlmesh.restart`, async () => {
      await restart()
    }),
    registerCommand(`sqlmesh.stop`, stop(lspClient)),
  )

  const result = await lspClient.start()
  if (isErr(result)) {
    return handleError(
      authProvider,
      restart,
      result.error,
      'Failed to start LSP',
    )
  } else {
    context.subscriptions.push(lspClient)
  }

  if (lspClient && !lspClient.hasCompletionCapability()) {
    context.subscriptions.push(
      vscode.languages.registerCompletionItemProvider(
        selector,
        completionProvider(lspClient),
      ),
    )
  }

  traceInfo('Extension activated')
}

// This method is called when your extension is deactivated
export async function deactivate() {
  if (lspClient) {
    await lspClient.dispose()
  }
}
