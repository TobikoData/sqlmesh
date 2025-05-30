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
import { renderModel } from './commands/renderModel'
import { isErr } from '@bus/result'
import {
  handleNotSginedInError,
  handleSqlmeshLspNotFoundError,
  handleSqlmeshLspDependenciesMissingError,
  handleTcloudBinNotFoundError,
} from './utilities/errors'
import { selector, completionProvider } from './completion/completion'
import { LineagePanel } from './webviews/lineagePanel'
import { RenderedModelProvider } from './providers/renderedModelProvider'

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
    vscode.commands.registerCommand('sqlmesh.signin', signIn(authProvider)),
  )
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.signinSpecifyFlow',
      signInSpecifyFlow(authProvider),
    ),
  )
  context.subscriptions.push(
    vscode.commands.registerCommand('sqlmesh.signout', signOut(authProvider)),
  )
  context.subscriptions.push(
    vscode.commands.registerCommand('sqlmesh.format', format(authProvider)),
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

  context.subscriptions.push(
    vscode.languages.registerCompletionItemProvider(
      selector,
      completionProvider(lspClient),
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

  const restart = async () => {
    if (lspClient) {
      traceVerbose('Restarting LSP client')
      const restartResult = await lspClient.restart()
      if (isErr(restartResult)) {
        switch (restartResult.error.type) {
          case 'not_signed_in':
            await handleNotSginedInError(authProvider)
            return
          case 'sqlmesh_lsp_not_found':
            await handleSqlmeshLspNotFoundError()
            return
          case 'sqlmesh_lsp_dependencies_missing':
            await handleSqlmeshLspDependenciesMissingError(restartResult.error)
            return
          case 'tcloud_bin_not_found':
            await handleTcloudBinNotFoundError()
            return
          case 'generic':
            await vscode.window.showErrorMessage(
              `Failed to restart LSP: ${restartResult.error.message}`,
            )
            return
        }
      }
      context.subscriptions.push(lspClient)
    }
  }

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
  )

  const result = await lspClient.start()
  if (isErr(result)) {
    switch (result.error.type) {
      case 'not_signed_in':
        await handleNotSginedInError(authProvider)
        break
      case 'sqlmesh_lsp_not_found':
        await handleSqlmeshLspNotFoundError()
        break
      case 'sqlmesh_lsp_dependencies_missing':
        await handleSqlmeshLspDependenciesMissingError(result.error)
        break
      case 'tcloud_bin_not_found':
        await handleTcloudBinNotFoundError()
        break
      case 'generic':
        await vscode.window.showErrorMessage(
          `Failed to start LSP: ${result.error.message}`,
        )
        break
    }
  } else {
    context.subscriptions.push(lspClient)
  }

  traceInfo('Extension activated')
}

// This method is called when your extension is deactivated
export async function deactivate() {
  if (lspClient) {
    await lspClient.dispose()
  }
}
