// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from "vscode"
import { actual_callout } from "./commands/format"
import {
  createOutputChannel,
  onDidChangeConfiguration,
  registerCommand,
} from "./utilities/common/vscodeapi"
import {
  registerLogger,
  traceInfo,
  traceVerbose,
} from "./utilities/common/log"
import {
 onDidChangePythonInterpreter,
} from "./utilities/common/python"
import { LSPClient } from "./lsp/lsp"

let lspClient: LSPClient | undefined

// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export async function activate(context: vscode.ExtensionContext) {
  const extensionOutputChannel = createOutputChannel("sqlmesh")
  context.subscriptions.push(
    extensionOutputChannel,
    registerLogger(extensionOutputChannel)
  )
  traceInfo("Activating SQLMesh extension")

  context.subscriptions.push(vscode.commands.registerCommand(
    "sqlmesh.format",
    async () => {
      const out = await actual_callout.format()
      if (out === 0) {
        vscode.window.showInformationMessage("Project formatted successfully")
      } else {
        vscode.window.showErrorMessage("Project format failed")
      }
    }
  ))

  lspClient = new LSPClient()
  await lspClient.start()
  context.subscriptions.push(lspClient)

  const restart = async () => {
    if (lspClient) {
      traceVerbose("Restarting LSP client")
      await lspClient.restart()
    }
  }

  context.subscriptions.push(
    onDidChangePythonInterpreter(async () => {
      await restart()
    }),
    onDidChangeConfiguration(async (e: vscode.ConfigurationChangeEvent) => {
      await restart()
    }),
    registerCommand(`sqlmesh.restart`, async () => {
      await restart()
    })
  )

 traceInfo("Extension activated")
}

// This method is called when your extension is deactivated
export async function deactivate() {
  if (lspClient) {
    await lspClient.dispose()
  }
}
