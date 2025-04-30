import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import * as vscode from 'vscode'

export const signOut =
  (authenticationProvider: AuthenticationProviderTobikoCloud) => async () => {
    await authenticationProvider.removeSession()
    await vscode.window.showInformationMessage('Signed out successfully')
  }
