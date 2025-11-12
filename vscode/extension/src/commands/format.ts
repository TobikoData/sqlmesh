import { traceLog } from '../utilities/common/log'
import { err, isErr, ok, Result } from '@bus/result'
import * as vscode from 'vscode'
import { ErrorType, handleError } from '../utilities/errors'
import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import { LSPClient } from '../lsp/lsp'

export const format =
  (
    authProvider: AuthenticationProviderTobikoCloud,
    lsp: LSPClient | undefined,
    restartLSP: () => Promise<void>,
  ) =>
  async (): Promise<void> => {
    traceLog('Calling format')
    const out = await internalFormat(lsp)
    if (isErr(out)) {
      return handleError(
        authProvider,
        restartLSP,
        out.error,
        'Project format failed',
      )
    }
    vscode.window.showInformationMessage('Project formatted successfully')
  }

const internalFormat = async (
  lsp: LSPClient | undefined,
): Promise<Result<undefined, ErrorType>> => {
  if (lsp === undefined) {
    return err({
      type: 'generic',
      message: 'LSP is not available',
    })
  }
  const response = await lsp.call_custom_method('sqlmesh/format_project', {})
  if (isErr(response)) {
    return response
  }
  return ok(undefined)
}
