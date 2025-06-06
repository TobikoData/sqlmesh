import { traceLog } from '../utilities/common/log'
import { sqlmeshExec } from '../utilities/sqlmesh/sqlmesh'
import { err, isErr, ok, Result } from '@bus/result'
import * as vscode from 'vscode'
import { ErrorType, handleError } from '../utilities/errors'
import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import { execAsync } from '../utilities/exec'

export const format =
  (authProvider: AuthenticationProviderTobikoCloud) =>
  async (): Promise<void> => {
    traceLog('Calling format')
    const out = await internalFormat()
    if (isErr(out)) {
      return handleError(authProvider, out.error, 'Project format failed')
    }
    vscode.window.showInformationMessage('Project formatted successfully')
  }

const internalFormat = async (): Promise<Result<undefined, ErrorType>> => {
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
