import { window } from 'vscode'
import { LSPClient } from '../lsp/lsp'
import { traceInfo } from '../utilities/common/log'

export const stop = (lspClient: LSPClient | undefined) => {
  return async () => {
    traceInfo('Stopping LSP server')

    if (!lspClient) {
      await window.showInformationMessage('LSP server is not running')
      return
    }

    await lspClient.stop()
    await window.showInformationMessage('LSP server stopped')
    traceInfo('LSP server stopped successfully')
  }
}
