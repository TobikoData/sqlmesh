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
  | { type: 'sqlmesh_lsp_not_found' }
  // sqlmesh_lsp_dependencies_missing is used when the sqlmesh_lsp is found but the lsp extras are missing.
  | SqlmeshLspDependenciesMissingError

interface SqlmeshLspDependenciesMissingError {
  type: 'sqlmesh_lsp_dependencies_missing'
  is_missing_pygls: boolean
  is_missing_lsprotocol: boolean
  is_tobiko_cloud: boolean
}

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

/**
 * Handles the case where the sqlmesh_lsp is not found.
 */
export const handleSqlmeshLspNotFoundError = async (): Promise<void> => {
  traceInfo('handleSqlmeshLspNotFoundError')
  await window.showErrorMessage(
    'SQLMesh LSP not found, please check installation',
  )
}

/**
 * Handles the case where the sqlmesh_lsp is found but the lsp extras are missing.
 */
export const handleSqlmeshLspDependenciesMissingError = async (
  error: SqlmeshLspDependenciesMissingError,
): Promise<void> => {
  traceInfo('handleSqlmeshLspDependenciesMissingError')
  if (error.is_tobiko_cloud) {
    await window.showErrorMessage(
      'LSP dependencies missing, make sure to include `lsp` in the `extras` section of your `tcloud.yaml` file.',
    )
  } else {
    const install = await window.showErrorMessage(
      'LSP dependencies missing, make sure to install `sqlmesh[lsp]`.',
      'Install',
    )
    if (install === 'Install') {
      const terminal = window.createTerminal({
        name: 'SQLMesh LSP Install',
        hideFromUser: false,
      })
      terminal.show()
      terminal.sendText("pip install 'sqlmesh[lsp]'", false)
    }
  }
}
