import { window } from 'vscode'
import { AuthenticationProviderTobikoCloud } from '../auth/auth'
import { signIn } from '../commands/signin'
import { traceInfo } from './common/log'

/**
 * Represents different types of errors that can occur in the application.
 */
export type ErrorType =
  | { type: 'generic'; message: string }
  | { type: 'not_signed_in' }

/**
 * Handles the case where the user is not signed in to Tobiko Cloud.
 * @param authProvider - The authentication provider to use for signing in.
 */
export const handleNotSginedInError = async (
  authProvider: AuthenticationProviderTobikoCloud,
): Promise<void> => {
  traceInfo('handleNotSginedInError')
  const result = await window.showInformationMessage(
    'Please sign in to Tobiko Cloud to use SQLMesh',
    'Sign In',
  )
  if (result === 'Sign In') {
    await signIn(authProvider)()
  }
}
