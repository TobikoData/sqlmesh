import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import * as vscode from 'vscode'
import { isCodespaces } from '../utilities/isCodespaces'
import { traceInfo } from '../utilities/common/log'

export const signIn =
  (
    authenticationProvider: AuthenticationProviderTobikoCloud,
    onSignInSuccess: () => Promise<void>,
  ) =>
  async () => {
    if (isCodespaces()) {
      await authenticationProvider.sign_in_device_flow()
    } else {
      await authenticationProvider.createSession()
    }

    // Do not await this, as this will block the thread, you just need to show the message, but not block
    vscode.window.showInformationMessage('Signed in successfully')

    // Execute callback after successful sign-in
    if (onSignInSuccess) {
      traceInfo('Executing post sign-in callback')
      try {
        await onSignInSuccess()
      } catch (error) {
        traceInfo(`Error in post sign-in callback: ${error}`)
      }
    }
  }
