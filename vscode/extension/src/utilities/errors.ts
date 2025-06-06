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
  // tcloud_bin_not_found is used when the tcloud executable is not found. This is likely to happen if the user
  // opens a project that has a `tcloud.yaml` file but doesn't have tcloud installed.
  | { type: 'tcloud_bin_not_found' }
  | SqlmeshLspDependenciesMissingError

/**
 * SqlmeshLspDependenciesMissingError is used when the sqlmesh_lsp is found but
 * the lsp extras are missing.
 */
interface SqlmeshLspDependenciesMissingError {
  type: 'sqlmesh_lsp_dependencies_missing'
  is_missing_pygls: boolean
  is_missing_lsprotocol: boolean
  is_tobiko_cloud: boolean
}

export async function handleError(
  authProvider: AuthenticationProviderTobikoCloud,
  error: ErrorType,
  genericErrorPrefix?: string,
): Promise<void> {
  traceInfo('handleError', error)
  switch (error.type) {
    case 'not_signed_in':
      return handleNotSignedInError(authProvider)
    case 'sqlmesh_lsp_not_found':
      return handleSqlmeshLspNotFoundError()
    case 'sqlmesh_lsp_dependencies_missing':
      return handleSqlmeshLspDependenciesMissingError(error)
    case 'tcloud_bin_not_found':
      return handleTcloudBinNotFoundError()
    case 'generic':
      if (genericErrorPrefix) {
        await window.showErrorMessage(`${genericErrorPrefix}: ${error.message}`)
      } else {
        await window.showErrorMessage(`An error occurred: ${error.message}`)
      }
      return
  }
}

/**
 * Handles the case where the user is not signed in to Tobiko Cloud.
 * @param authProvider - The authentication provider to use for signing in.
 */
const handleNotSignedInError = async (
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
const handleSqlmeshLspNotFoundError = async (): Promise<void> => {
  traceInfo('handleSqlmeshLspNotFoundError')
  await window.showErrorMessage(
    'SQLMesh LSP not found, please check installation',
  )
}

/**
 * Handles the case where the sqlmesh_lsp is found but the lsp extras are missing.
 */
const handleSqlmeshLspDependenciesMissingError = async (
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

/**
 * Handles the case where the tcloud executable is not found.
 */
const handleTcloudBinNotFoundError = async (): Promise<void> => {
  const result = await window.showErrorMessage(
    'tcloud executable not found, please check installation',
    'Install',
  )
  if (result === 'Install') {
    const terminal = window.createTerminal({
      name: 'Tcloud Install',
      hideFromUser: false,
    })
    terminal.show()
    terminal.sendText('pip install tcloud', false)
  }
}
