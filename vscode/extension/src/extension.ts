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
import { isErr } from './utilities/functional/result'
import { handleNotSginedInError } from './utilities/errors'

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
  const result = await lspClient.start()
  if (isErr(result)) {
    handleNotSginedInError(authProvider)
  }
  context.subscriptions.push(lspClient)

  const restart = async () => {
    if (lspClient) {
      traceVerbose('Restarting LSP client')
      const restartResult = await lspClient.restart()
      if (isErr(restartResult)) {
        handleNotSginedInError(authProvider)
      }
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

  traceInfo('Extension activated')
}

// This method is called when your extension is deactivated
export async function deactivate() {
  if (lspClient) {
    await lspClient.dispose()
  }
}
