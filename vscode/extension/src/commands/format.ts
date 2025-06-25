import { traceLog } from '../utilities/common/log'
import { sqlmeshExec } from '../utilities/sqlmesh/sqlmesh'
import { err, isErr, ok, Result } from '@bus/result'
import * as vscode from 'vscode'
import { ErrorType, handleError } from '../utilities/errors'
import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import { execAsync } from '../utilities/exec'
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
  try {
    // Try LSP method first
    if (lsp) {
      const response = await lsp.call_custom_method(
        'sqlmesh/format_project',
        {},
      )
      if (isErr(response)) {
        return response
      }
      return ok(undefined)
    }
  } catch (error) {
    traceLog(`LSP format failed, falling back to CLI: ${JSON.stringify(error)}`)
  }

  // Fallback to CLI method if LSP is not available
  // TODO This is a solution in order to be backwards compatible in the cases
  //  where the LSP method is not implemented yet. This should be removed at
  //  some point in the future.
  const exec = await sqlmeshExec()
  if (isErr(exec)) {
    return exec
  }
  const result = await execAsync(`${exec.value.bin}`, ['format'], {
    cwd: exec.value.workspacePath,
    env: exec.value.env,
  })
  if (result.exitCode !== 0) {
    return err({
      type: 'generic',
      message: `Error executing sqlmesh format: ${result.stderr}`,
    })
  }
  return ok(undefined)
}
