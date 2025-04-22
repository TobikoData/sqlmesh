import { traceLog } from "../utilities/common/log"
import { execSync } from "child_process"
import { sqlmesh_exec } from "../utilities/sqlmesh/sqlmesh"
import { err, isErr, ok, Result } from "../utilities/functional/result"
import * as vscode from "vscode"
import { ErrorType, handleNotSginedInError } from "../utilities/errors"
import { AuthenticationProviderTobikoCloud } from "../auth/auth"

export const format =
  (authProvider: AuthenticationProviderTobikoCloud) => async () => {
    traceLog("Calling format")
    const out = await internalFormat()
    if (isErr(out)) {
      if (out.error.type === "not_signed_in") {
        handleNotSginedInError(authProvider)
        return
      } else {
        vscode.window.showErrorMessage(
          `Project format failed: ${out.error.message}`
        )
        return
      }
    }
    vscode.window.showInformationMessage("Project formatted successfully")
  }

const internalFormat = async (): Promise<Result<number, ErrorType>> => {
  try {
    const exec = await sqlmesh_exec()
    if (isErr(exec)) {
      return exec
    }
    execSync(`${exec.value.bin} format`, {
      encoding: "utf-8",
      cwd: exec.value.workspacePath,
      env: exec.value.env,
    })
    return ok(0)
  } catch (error: any) {
    return err({
      type: "generic",
      message: `Error executing sqlmesh format: ${error.message}`,
    })
  }
}
