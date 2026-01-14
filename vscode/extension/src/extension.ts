/**********************************************************************
 *  Extension entry point                                              *
 *********************************************************************/

import * as vscode from 'vscode'

import { format } from './commands/format'
import { signOut } from './commands/signout'
import { signIn } from './commands/signin'
import { signInSpecifyFlow } from './commands/signinSpecifyFlow'
import { renderModel, reRenderModelForSourceFile } from './commands/renderModel'
import { stop } from './commands/stop'
import { printEnvironment } from './commands/printEnvironment'

import {
  createOutputChannel,
  onDidChangeConfiguration,
  registerCommand,
} from './utilities/common/vscodeapi'
import {
  registerLogger,
  traceInfo,
  traceVerbose,
  traceError,
} from './utilities/common/log'
import { onDidChangePythonInterpreter } from './utilities/common/python'
import { sleep } from './utilities/sleep'
import { handleError } from './utilities/errors'

import { selector, completionProvider } from './completion/completion'
import { LineagePanel } from './webviews/lineagePanel'
import { RenderedModelProvider } from './providers/renderedModelProvider'
import { showTableDiff } from './commands/tableDiff'

import {
  controller as testController,
  setupTestController,
} from './tests/tests'

import { isErr } from '@bus/result'
import { AuthenticationProviderTobikoCloud } from './auth/auth'
import { LSPClient } from './lsp/lsp'

/** Singleton LSP client for the extension. */
let lspClient: LSPClient | undefined

/** Handle to the (single) test controller disposable so we can replace it on restart. */
let testControllerDisposable: vscode.Disposable | undefined

export async function activate(context: vscode.ExtensionContext) {
  const extensionOutputChannel = createOutputChannel('sqlmesh')
  context.subscriptions.push(
    extensionOutputChannel,
    registerLogger(extensionOutputChannel),
  )
  traceInfo('Activating SQLMesh extension')

  const authProvider = new AuthenticationProviderTobikoCloud()
  context.subscriptions.push(
    vscode.authentication.registerAuthenticationProvider(
      AuthenticationProviderTobikoCloud.id,
      AuthenticationProviderTobikoCloud.name,
      authProvider,
      { supportsMultipleAccounts: false },
    ),
  )

  const restartLsp = async (invokedByUser = false): Promise<void> => {
    if (!lspClient) {
      lspClient = new LSPClient()
    }

    traceVerbose('Restarting SQLMesh LSP client')
    const result = await lspClient.restart(invokedByUser)
    if (isErr(result)) {
      await handleError(
        authProvider,
        restartLsp,
        result.error,
        'LSP restart failed',
      )
      return
    }

    // push once to avoid duplicate disposables on multiple restarts
    if (!context.subscriptions.includes(lspClient)) {
      context.subscriptions.push(lspClient)
    }

    /* Replace the test controller each time we restart the client */
    if (testControllerDisposable) {
      testControllerDisposable.dispose()
    }
    testControllerDisposable = setupTestController(lspClient)
    context.subscriptions.push(testControllerDisposable)
  }

  // commands needing the restart helper
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.signin',
      signIn(authProvider, () => restartLsp()),
    ),
    vscode.commands.registerCommand(
      'sqlmesh.signinSpecifyFlow',
      signInSpecifyFlow(authProvider, () => restartLsp()),
    ),
    vscode.commands.registerCommand('sqlmesh.signout', signOut(authProvider)),
  )

  //  Instantiate the LSP client (once)
  lspClient = new LSPClient()
  const startResult = await lspClient.start()
  if (isErr(startResult)) {
    await handleError(
      authProvider,
      restartLsp,
      startResult.error,
      'Failed to start LSP',
    )
    return // abort activation – nothing else to do
  }

  context.subscriptions.push(lspClient)

  // Initialize the test controller
  testControllerDisposable = setupTestController(lspClient)
  context.subscriptions.push(testControllerDisposable, testController)

  // Register the rendered model provider
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

  const lineagePanel = new LineagePanel(context.extensionUri, lspClient)
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(
      LineagePanel.viewType,
      lineagePanel,
    ),
  )

  // Register the table diff command
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.showTableDiff',
      showTableDiff(lspClient, context.extensionUri),
    ),
  )

  // Re‑render model automatically when its source file is saved
  context.subscriptions.push(
    vscode.workspace.onDidSaveTextDocument(async document => {
      if (
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

  // miscellaneous commands
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'sqlmesh.format',
      format(authProvider, lspClient, restartLsp),
    ),
    registerCommand('sqlmesh.restart', () => restartLsp(true)),
    registerCommand('sqlmesh.stop', stop(lspClient)),
    registerCommand('sqlmesh.printEnvironment', printEnvironment()),
  )

  context.subscriptions.push(
    onDidChangePythonInterpreter(() => restartLsp()),
    onDidChangeConfiguration(() => restartLsp()),
  )

  if (!lspClient.hasCompletionCapability()) {
    context.subscriptions.push(
      vscode.languages.registerCompletionItemProvider(
        selector,
        completionProvider(lspClient),
      ),
    )
  }

  traceInfo('SQLMesh extension activated')
}

// This method is called when your extension is deactivated
export async function deactivate() {
  try {
    if (testControllerDisposable) {
      testControllerDisposable.dispose()
    }
    if (lspClient) {
      await lspClient.dispose()
    }
  } catch (e) {
    traceError(`Error during deactivate: ${e}`)
  }
}
