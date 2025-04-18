import {
  env,
  Uri,
  AuthenticationProvider,
  AuthenticationProviderAuthenticationSessionsChangeEvent,
  AuthenticationSession,
  Event,
  EventEmitter,
  window,
} from "vscode"
import { get_tcloud_bin } from "../utilities/sqlmesh/sqlmesh"
import { err, isErr, ok, Result } from "../utilities/functional/result"
import { execAsync } from "../utilities/exec"
import { getProjectRoot } from "../utilities/common/utilities"
import z from "zod"
import { traceError } from "../utilities/common/log"

export const AUTH_TYPE = "tobikodata"
export const AUTH_NAME = "Tobiko"

const tokenSchema = z.object({
    iss: z.string(),
    aud: z.string(),
    sub: z.string(),
    scope: z.string(),
    iat: z.number(),
    exp: z.number(),
    email: z.string(),
})
const statusResponseSchema = z.object({
  is_logged_in: z.boolean(),
  id_token: tokenSchema,
})

type StatusResponse = z.infer<typeof statusResponseSchema>;

const loginUrlResponseSchema = z.object({
  url: z.string(),
  verifier_code: z.string(),
})

export class AuthenticationProviderTobikoCloud
  implements AuthenticationProvider
{
  static id = AUTH_TYPE
  static name = AUTH_NAME

  private _sessionChangeEmitter =
    new EventEmitter<AuthenticationProviderAuthenticationSessionsChangeEvent>()

  onDidChangeSessions: Event<AuthenticationProviderAuthenticationSessionsChangeEvent> =
    this._sessionChangeEmitter.event

  /**
   * Get the status of the authentication provider from the cli
   * @returns true if the user is logged in with the id token, false otherwise
   */
  private async get_status(): Promise<Result<StatusResponse, string>> {
    const workspacePath = await getProjectRoot()
    const tcloudBin = await get_tcloud_bin()
    if (isErr(tcloudBin)) {
      return err(tcloudBin.error)
    }
    const tcloudBinPath = tcloudBin.value
    const result = await execAsync(
      tcloudBinPath,
      ["auth", "vscode", "status"],
      {
        cwd: workspacePath.uri.fsPath,
      }
    )
    if (result.exitCode !== 0) {
      return err("Failed to get tcloud auth status")
    }
    const status = result.stdout
    const statusToJson = JSON.parse(status)
    const statusResponse = statusResponseSchema.parse(statusToJson)
    return ok(statusResponse)
  }

  async getSessions(): Promise<AuthenticationSession[]> {
    const status = await this.get_status()
    if (isErr(status)) {
      return []
    }
    const statusResponse = status.value
    if (!statusResponse.is_logged_in) {
      return []
    }
    const token = statusResponse.id_token
    const session = {
      id: token.email,
      account: {
        id: token.email,
        label: "Tobiko",
      },
      scopes: token.scope.split(" "),
      accessToken: "",
    }
    return [session]
  }

  async createSession(): Promise<AuthenticationSession> {
    const workspacePath = await getProjectRoot()
    const tcloudBin = await get_tcloud_bin()
    if (isErr(tcloudBin)) {
      throw new Error("Failed to get tcloud bin")
    }
    const tcloudBinPath = tcloudBin.value
    const result = await execAsync(
      tcloudBinPath,
      ["auth", "vscode", "login-url"],
      {
        cwd: workspacePath.uri.fsPath,
      }
    )
    if (result.exitCode !== 0) {
      throw new Error("Failed to get tcloud login url")
    }
    const resultToJson = JSON.parse(result.stdout)
    const urlCode = loginUrlResponseSchema.parse(resultToJson)
    const url = urlCode.url

    const ac = new AbortController()
    const timeout = setTimeout(() => ac.abort(), 1000 * 60 * 5)
    const backgroundServerForLogin = execAsync(
      tcloudBinPath,
      ["auth", "vscode", "start-server", urlCode.verifier_code],
      {
        cwd: workspacePath.uri.fsPath,
        signal: ac.signal,
      }
    )

    const messageResult = await window.showInformationMessage(
      "Please login to Tobiko Cloud",
      {
        modal: true,
      },
      "Sign in with browser",
      "Cancel"
    )

    if (messageResult === "Sign in with browser") {
      await env.openExternal(Uri.parse(url))
    }
    if (messageResult === "Cancel") {
      ac.abort()
      throw new Error("Login cancelled")
    }

    try {
      const output = await backgroundServerForLogin
      if (output.exitCode !== 0) {
        throw new Error(`Failed to start server: ${output.stderr}`)
      }
    } catch (error) {
      traceError(`Server error: ${error}`)
      throw error
    }

    clearTimeout(timeout)

    const status = await this.get_status()
    if (isErr(status)) {
      throw new Error("Failed to get tcloud auth status")
    }
    const statusResponse = status.value
    if (!statusResponse.is_logged_in) {
      throw new Error("Failed to login to tcloud")
    }
    const scopes = statusResponse.id_token.scope.split(" ")
    const session: AuthenticationSession = {
      id: AuthenticationProviderTobikoCloud.id,
      account: {
        id: AuthenticationProviderTobikoCloud.id,
        label: "Tobiko",
      },
      scopes: scopes,
      accessToken: ""
    }
    return session
  }

  async removeSession(): Promise<void> {
    const tcloudBin = await get_tcloud_bin()
    const workspacePath = await getProjectRoot()
    if (isErr(tcloudBin)) {
      throw new Error("Failed to get tcloud bin")
    }
    const tcloudBinPath = tcloudBin.value
    const result = await execAsync(tcloudBinPath, ["auth", "logout"], {
      cwd: workspacePath.uri.fsPath,
    })
    if (result.exitCode !== 0) {
      throw new Error("Failed to logout from tcloud")
    }
  }
}
