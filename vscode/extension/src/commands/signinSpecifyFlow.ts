import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import { traceInfo } from '../utilities/common/log'
import { window } from 'vscode'

export const signInSpecifyFlow =
  (
    authenticationProvider: AuthenticationProviderTobikoCloud,
    onSignInSuccess?: () => Promise<void>,
  ) =>
  async () => {
    traceInfo('Sign in specify flow')
    const flowOptions = [
      {
        label: 'OAuth Flow',
        description: 'Sign in using OAuth flow in your browser',
      },
      { label: 'Device Flow', description: 'Sign in using a device code' },
    ]
    const selectedFlow = await window.showQuickPick(flowOptions, {
      placeHolder: 'Select authentication flow method',
      ignoreFocusOut: true,
    })
    if (!selectedFlow) {
      traceInfo('Sign in cancelled by user')
      return
    }
    if (selectedFlow.label === 'OAuth Flow') {
      await authenticationProvider.sign_in_oauth_flow()
      await authenticationProvider.getSessions()
      await window.showInformationMessage('Sign in success')

      // Execute callback after successful sign-in
      if (onSignInSuccess) {
        traceInfo('Executing post sign-in callback')
        try {
          await onSignInSuccess()
        } catch (error) {
          traceInfo(`Error in post sign-in callback: ${error}`)
        }
      }
      return
    } else if (selectedFlow.label === 'Device Flow') {
      await authenticationProvider.sign_in_device_flow()
      await authenticationProvider.getSessions()
      await window.showInformationMessage('Sign in success')

      // Execute callback after successful sign-in
      if (onSignInSuccess) {
        traceInfo('Executing post sign-in callback')
        try {
          await onSignInSuccess()
        } catch (error) {
          traceInfo(`Error in post sign-in callback: ${error}`)
        }
      }
      return
    } else {
      traceInfo('Invalid flow selected')
      return
    }
  }
