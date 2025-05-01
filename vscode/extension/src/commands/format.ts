import { traceLog } from '../utilities/common/log'
import { execSync } from 'child_process'
import { sqlmesh_exec } from '../utilities/sqlmesh/sqlmesh'
import { err, isErr, ok, Result } from '../utilities/functional/result'
import * as vscode from 'vscode'
import {
  ErrorType,
  handleNotSginedInError,
  handleSqlmeshLspNotFoundError,
  handleSqlmeshLspDependenciesMissingError,
} from '../utilities/errors'
import { AuthenticationProviderTobikoCloud } from '../auth/auth'

export const format =
  (authProvider: AuthenticationProviderTobikoCloud) =>
  async (): Promise<void> => {
    traceLog('Calling format')
    const out = await internalFormat()
    if (isErr(out)) {
      switch (out.error.type) {
        case 'not_signed_in':
          await handleNotSginedInError(authProvider)
          return
        case 'sqlmesh_lsp_not_found':
          await handleSqlmeshLspNotFoundError()
          return
        case 'sqlmesh_lsp_dependencies_missing':
          await handleSqlmeshLspDependenciesMissingError(out.error)
          return
        case 'generic':
          await vscode.window.showErrorMessage(
            `Project format failed: ${out.error.message}`,
          )
          return
      }
    }
    vscode.window.showInformationMessage('Project formatted successfully')
  }

const internalFormat = async (): Promise<Result<number, ErrorType>> => {
  try {
    const exec = await sqlmesh_exec()
    if (isErr(exec)) {
      return exec
    }
    execSync(`${exec.value.bin} format`, {
      encoding: 'utf-8',
      cwd: exec.value.workspacePath,
      env: exec.value.env,
    })
    return ok(0)
  } catch (error: any) {
    return err({
      type: 'generic',
      message: `Error executing sqlmesh format: ${error.message}`,
    })
  }
}
