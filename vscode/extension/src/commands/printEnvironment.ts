import * as vscode from 'vscode'
import { getSqlmeshEnvironment } from '../utilities/sqlmesh/sqlmesh'
import { isErr } from '@bus/result'
import { IS_WINDOWS } from '../utilities/isWindows'

export function printEnvironment() {
  return async () => {
    const envResult = await getSqlmeshEnvironment()

    if (isErr(envResult)) {
      await vscode.window.showErrorMessage(envResult.error)
      return
    }

    const env = envResult.value

    // Create a new terminal with the SQLMesh environment
    const terminal = vscode.window.createTerminal({
      name: 'SQLMesh Environment',
      env: env,
    })

    // Show the terminal
    terminal.show()

    // Run the appropriate command to display environment variables
    if (IS_WINDOWS) {
      // On Windows, use 'set' command
      terminal.sendText('set')
    } else {
      // On Unix-like systems, use 'env' command
      terminal.sendText('env | sort')
    }

    // Show a notification
    vscode.window.showInformationMessage(
      'SQLMesh environment variables displayed in terminal',
    )
  }
}
