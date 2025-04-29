import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import * as vscode from 'vscode'
import { isCodespaces } from '../utilities/isCodespaces'

export const signIn =
  (authenticationProvider: AuthenticationProviderTobikoCloud) => async () => {
    if (isCodespaces()) {
      await authenticationProvider.sign_in_device_flow()
    } else {
      await authenticationProvider.createSession()
    }
    await vscode.window.showInformationMessage('Signed in successfully')
  }
