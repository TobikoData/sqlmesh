import path from 'path'
import { traceInfo, traceLog, traceVerbose } from '../common/log'
import { getInterpreterDetails, getPythonEnvVariables } from '../common/python'
import { Result, err, isErr, ok } from '@bus/result'
import { getProjectRoot } from '../common/utilities'
import { isPythonModuleInstalled } from '../python'
import fs from 'fs'
import { ErrorType } from '../errors'
import { isSignedIntoTobikoCloud } from '../../auth/auth'
import { execAsync } from '../exec'
import z from 'zod'
import { ProgressLocation, window } from 'vscode'
import { IS_WINDOWS } from '../isWindows'
import { resolveProjectPath } from '../config'
import { isSemVerGreaterThanOrEqual } from '../semver'

export interface SqlmeshExecInfo {
  workspacePath: string
  bin: string
  env: Record<string, string | undefined>
  args: string[]
}

/**
 * Returns true if the current project is a Tcloud project. To detect this we,
 * 1. Check if the project has a tcloud.yaml file in the project root. If it does, we assume it's a Tcloud project.
 * 2. Check if the project has tcloud installed in the Python environment.
 *
 * @returns A Result indicating whether tcloud is installed.
 */
export const isTcloudProject = async (): Promise<Result<boolean, string>> => {
  const projectRoot = await getProjectRoot()
  const resolvedPath = resolveProjectPath(projectRoot)
  if (isErr(resolvedPath)) {
    return err(resolvedPath.error)
  }
  const tcloudYamlPath = path.join(resolvedPath.value, 'tcloud.yaml')
  const tcloudYmlPath = path.join(resolvedPath.value, 'tcloud.yml')
  const isTcloudYamlFilePresent = fs.existsSync(tcloudYamlPath)
  const isTcloudYmlFilePresent = fs.existsSync(tcloudYmlPath)
  if (isTcloudYamlFilePresent || isTcloudYmlFilePresent) {
    traceVerbose(`tcloud yaml or yml file present at : ${tcloudYamlPath}`)
    return ok(true)
  }
  const isTcloudInstalled = await isPythonModuleInstalled('tcloud')
  if (isErr(isTcloudInstalled)) {
    return isTcloudInstalled
  }
  traceVerbose(`tcloud is installed: ${isTcloudInstalled.value}`)
  return ok(isTcloudInstalled.value)
}

/**
 * Get the tcloud executable for the current Python environment.
 *
 * @returns The tcloud executable for the current Python environment.
 */
export const getTcloudBin = async (): Promise<Result<SqlmeshExecInfo, ErrorType>> => {
  const tcloud = IS_WINDOWS ? 'tcloud.exe' : 'tcloud'
  const interpreterDetails = await getInterpreterDetails()
  if (!interpreterDetails.path) {
    return err({
      type: 'tcloud_bin_not_found',
    })
  }
  const pythonPath = interpreterDetails.path[0]
  const binPath = path.join(path.dirname(pythonPath), tcloud)
  if (!fs.existsSync(binPath)) {
    return err({type: 'tcloud_bin_not_found'})
  }
  const envVariables = await getPythonEnvVariables()
  if (isErr(envVariables)) {
    return err({
      type: 'generic',
      message: envVariables.error,
    })
  }
  return ok({
    bin: binPath,
    workspacePath: interpreterDetails.resource?.fsPath ?? '',
    env: {
      ...process.env,
      ...envVariables.value,
      PYTHONPATH: interpreterDetails.path[0],
      VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
      PATH: `${interpreterDetails.binPath!}${path.delimiter}${process.env.PATH || ''}`,
    },
    args: [],
  })
}

const isSqlmeshInstalledSchema = z.object({
  is_installed: z.boolean(),
})

/**
 * Returns true if the current project is a sqlmesh enterprise project is installed and updated.
 *
 * @returns A Result indicating whether sqlmesh enterprise is installed and updated.
 */
export const isSqlmeshEnterpriseInstalled = async (): Promise<
  Result<boolean, ErrorType>
> => {
  traceInfo('Checking if sqlmesh enterprise is installed')
  const tcloudBin = await getTcloudBin()
  if (isErr(tcloudBin)) {
    return tcloudBin
  }
  const projectRoot = await getProjectRoot()
  const resolvedPath = resolveProjectPath(projectRoot)
  if (isErr(resolvedPath)) {
    return err({
      type: 'generic',
      message: resolvedPath.error,
    })
  }
  const called = await execAsync(tcloudBin.value.bin, ['is_sqlmesh_installed'], {
    cwd: resolvedPath.value,
    env: tcloudBin.value.env,
  })
  if (called.exitCode !== 0) {
    return err({
      type: 'generic',
      message: `Failed to check if sqlmesh enterprise is installed: ${called.stderr}`,
    })
  }
  const parsed = isSqlmeshInstalledSchema.safeParse(JSON.parse(called.stdout))
  if (!parsed.success) {
    return err({
      type: 'generic',
      message: `Failed to parse sqlmesh enterprise installation status: ${parsed.error.message}`,
    })
  }
  return ok(parsed.data.is_installed)
}

/**
 * Install sqlmesh enterprise.
 *
 * @returns A Result indicating whether sqlmesh enterprise was installed.
 */
export const installSqlmeshEnterprise = async (
  abortController: AbortController,
): Promise<Result<boolean, ErrorType>> => {
  const tcloudBin = await getTcloudBin()
  if (isErr(tcloudBin)) {
    return tcloudBin
  }
  const projectRoot = await getProjectRoot()
  const resolvedPath = resolveProjectPath(projectRoot)
  if (isErr(resolvedPath)) {
    return err({
      type: 'generic',
      message: resolvedPath.error,
    })
  }
  const called = await execAsync(tcloudBin.value.bin, ['install_sqlmesh'], {
    signal: abortController.signal,
    cwd: resolvedPath.value,
    env: tcloudBin.value.env,
  })
  if (called.exitCode !== 0) {
    return err({
      type: 'generic',
      message: `Failed to install sqlmesh enterprise: ${called.stderr}`,
    })
  }
  return ok(true)
}

let installationLock: Promise<Result<boolean, ErrorType>> | undefined = undefined

/**
 * Checks if sqlmesh enterprise is installed and updated. If not, it will install it.
 * This will also create a progress message in vscode in order to inform the user that sqlmesh enterprise is being installed.
 * Uses a lock mechanism to prevent parallel executions.
 *
 * @returns A Result indicating whether sqlmesh enterprise was installed in the call.
 */
export const ensureSqlmeshEnterpriseInstalled = async (): Promise<
  Result<boolean, ErrorType>
> => {
  // If there's an ongoing installation, wait for it to complete
  if (installationLock) {
    return installationLock
  }

  // Create a new lock
  installationLock = (async () => {
    try {
      traceInfo('Ensuring sqlmesh enterprise is installed')
      const isInstalled = await isSqlmeshEnterpriseInstalled()
      if (isErr(isInstalled)) {
        return isInstalled
      }
      if (isInstalled.value) {
        traceInfo('Sqlmesh enterprise is installed')
        return ok(false)
      }
      traceInfo('Sqlmesh enterprise is not installed, installing...')
      const abortController = new AbortController()
      const installResult = await window.withProgress(
        {
          location: ProgressLocation.Notification,
          title: 'SQLMesh',
          cancellable: true,
        },
        async (progress, token) => {
          // Connect the cancellation token to our abort controller
          token.onCancellationRequested(() => {
            abortController.abort()
            traceInfo('Sqlmesh enterprise installation cancelled')
            window.showInformationMessage('Installation cancelled')
          })
          progress.report({ message: 'Installing enterprise python package...' })
          const result = await installSqlmeshEnterprise(abortController)
          if (isErr(result)) {
            return result
          }
          return ok(true)
        },
      )
      if (isErr(installResult)) {
        return installResult
      }
      return ok(true)
    } finally {
      // Clear the lock when done
      installationLock = undefined
    }
  })()

  return installationLock
}

/**
 * Get the sqlmesh executable for the current workspace.
 *
 * @deprecated Use LSP instead of direct sqlmesh execution for any new functionality.
 */
export const sqlmeshExec = async (): Promise<
  Result<SqlmeshExecInfo, ErrorType>
> => {
  const sqlmesh = IS_WINDOWS ? 'sqlmesh.exe' : 'sqlmesh'
  const projectRoot = await getProjectRoot()
  const resolvedPath = resolveProjectPath(projectRoot)
  if (isErr(resolvedPath)) {
    return err({
      type: 'generic',
      message: resolvedPath.error,
    })
  }
  const envVariables = await getPythonEnvVariables()
  if (isErr(envVariables)) {
    return err({
      type: 'generic',
      message: envVariables.error,
    })
  }
  const workspacePath = resolvedPath.value
  const interpreterDetails = await getInterpreterDetails()
  traceLog(`Interpreter details: ${JSON.stringify(interpreterDetails)}`)
  if (interpreterDetails.path) {
    traceVerbose(
      `Using interpreter from Python extension: ${interpreterDetails.path.join(
        ' ',
      )}`,
    )
  }
  if (interpreterDetails.isVirtualEnvironment) {
    traceLog('Using virtual environment')
    const isTcloudInstalled = await isTcloudProject()
    if (isErr(isTcloudInstalled)) {
      return err({
        type: 'generic',
        message: isTcloudInstalled.error,
      })
    }
    if (isTcloudInstalled.value) {
      const tcloudBin = await getTcloudBin()
      if (isErr(tcloudBin)) {
        return tcloudBin
      }
      const isSignedIn = await isSignedIntoTobikoCloud()
      if (!isSignedIn) {
        return err({
          type: 'not_signed_in',
        })
      }
      const ensured = await ensureSqlmeshEnterpriseInstalled()
      if (isErr(ensured)) {
        return ensured
      }
      return ok({
        bin: tcloudBin.value.bin,
        workspacePath,
        env: tcloudBin.value.env,
        args: ["sqlmesh"],
      })
    }
    const binPath = path.join(interpreterDetails.binPath!, sqlmesh)
    traceLog(`Bin path: ${binPath}`)
    return ok({
      bin: binPath,
      workspacePath,
      env: {
        ...process.env,
        ...envVariables.value,
        PYTHONPATH: interpreterDetails.path?.[0],
        VIRTUAL_ENV: path.dirname(path.dirname(interpreterDetails.binPath!)), // binPath now points to bin dir
        PATH: `${interpreterDetails.binPath!}${path.delimiter}${process.env.PATH || ''}`,
      },
      args: [],
    })
  } else {
    const exists = await doesExecutableExist(sqlmesh)
    if (!exists) {
      return err({
        type: 'sqlmesh_not_found',
      })
    }
    return ok({
      bin: sqlmesh,
      workspacePath,
      env: {
        ...process.env,
        ...envVariables.value,
      },
      args: [],
    })
  }
}

/**
 * Ensure that the sqlmesh_lsp dependencies are installed.
 *
 * @returns A Result indicating whether the sqlmesh_lsp dependencies were installed.
 */
export const ensureSqlmeshLspDependenciesInstalled = async (): Promise<
  Result<undefined, ErrorType>
> => {
  const isPyglsInstalled = await isPythonModuleInstalled('pygls')
  if (isErr(isPyglsInstalled)) {
    return err({
      type: 'generic',
      message: isPyglsInstalled.error,
    })
  }
  const isLsprotocolInstalled = await isPythonModuleInstalled('lsprotocol')
  if (isErr(isLsprotocolInstalled)) {
    return err({
      type: 'generic',
      message: isLsprotocolInstalled.error,
    })
  }
  const isTobikoCloudInstalled = await isTcloudProject()
  if (isErr(isTobikoCloudInstalled)) {
    return err({
      type: 'generic',
      message: isTobikoCloudInstalled.error,
    })
  }
  if (!isPyglsInstalled.value || !isLsprotocolInstalled.value) {
    return err({
      type: 'sqlmesh_lsp_dependencies_missing',
      is_missing_pygls: !isPyglsInstalled.value,
      is_missing_lsprotocol: !isLsprotocolInstalled.value,
      is_tobiko_cloud: isTobikoCloudInstalled.value,
    })
  }
  return ok(undefined)
}

/**
 * Get the sqlmesh_lsp executable for the current workspace.
 *
 * @returns The sqlmesh_lsp executable for the current workspace.
 */
export const sqlmeshLspExec = async (): Promise<
  Result<SqlmeshExecInfo, ErrorType>
> => {
  const sqlmeshLSP = IS_WINDOWS ? 'sqlmesh_lsp.exe' : 'sqlmesh_lsp'
  const projectRoot = await getProjectRoot()
  const envVariables = await getPythonEnvVariables()
  if (isErr(envVariables)) {
    return err({
      type: 'generic',
      message: envVariables.error,
    })
  }
  const resolvedPath = resolveProjectPath(projectRoot)
  if (isErr(resolvedPath)) {
    return err({
      type: 'generic',
      message: resolvedPath.error,
    })
  }
  const workspacePath = resolvedPath.value
  const interpreterDetails = await getInterpreterDetails()
  traceLog(`Interpreter details: ${JSON.stringify(interpreterDetails)}`)
  if (interpreterDetails.path) {
    traceVerbose(
      `Using interpreter from Python extension: ${interpreterDetails.path.join(
        ' ',
      )}`,
    )
  }
  if (interpreterDetails.isVirtualEnvironment) {
    traceLog('Using virtual environment')
    const tcloudInstalled = await isTcloudProject()
    if (isErr(tcloudInstalled)) {
      return err({
        type: 'generic',
        message: tcloudInstalled.error,
      })
    }
    if (tcloudInstalled.value) {
      traceLog('Tcloud installed, installing sqlmesh')
      const tcloudBin = await getTcloudBin()
      if (isErr(tcloudBin)) {
        return tcloudBin
      }
      const isSignedIn = await isSignedIntoTobikoCloud()
      if (!isSignedIn) {
        return err({
          type: 'not_signed_in',
        })
      }
      const ensured = await ensureSqlmeshEnterpriseInstalled()
      if (isErr(ensured)) {
        return ensured
      }
      const tcloudBinVersion = await getTcloudBinVersion()
      if (isErr(tcloudBinVersion)) {
        return tcloudBinVersion
      }
      // TODO: Remove this once we have a stable version of tcloud that supports sqlmesh_lsp.
      if (isSemVerGreaterThanOrEqual(tcloudBinVersion.value, [2, 10, 1])) {
        return ok ({
          bin: tcloudBin.value.bin,
          workspacePath,
          env: tcloudBin.value.env,
          args: ['sqlmesh_lsp'],
        })
      }
    }
    const binPath = path.join(interpreterDetails.binPath!, sqlmeshLSP)
    traceLog(`Bin path: ${binPath}`)
    if (!fs.existsSync(binPath)) {
      return err({
        type: 'sqlmesh_lsp_not_found',
      })
    }
    const ensuredDependencies = await ensureSqlmeshLspDependenciesInstalled()
    if (isErr(ensuredDependencies)) {
      return ensuredDependencies
    }
    return ok({
      bin: binPath,
      workspacePath,
      env: {
        ...process.env, 
        ...envVariables.value,
        PYTHONPATH: interpreterDetails.path?.[0],
        VIRTUAL_ENV: path.dirname(path.dirname(interpreterDetails.binPath!)), // binPath now points to bin dir
        PATH: `${interpreterDetails.binPath!}${path.delimiter}${process.env.PATH || ''}`, // binPath already points to the bin directory
      },
      args: [],
    })
  } else {
    const exists = await doesExecutableExist(sqlmeshLSP)
    if (!exists) {
      return err({
        type: 'sqlmesh_lsp_not_found',
      })
    }
    return ok({
      bin: sqlmeshLSP,
      workspacePath,
      env: {
        ...process.env,
        ...envVariables.value,
      },
      args: [],
    })
  }
}

async function doesExecutableExist(executable: string): Promise<boolean> {
  const command = process.platform === 'win32' ? 'where.exe' : 'which'
  traceLog(`Checking if ${executable} exists with ${command}`)
  try {
    const result = await execAsync(command, [executable])
    traceLog(`Checked if ${executable} exists with ${command}, with result ${result.exitCode}`)
    const exists = result.exitCode === 0
    traceLog(`Checked if ${executable} exists with ${command}, with result ${exists}`)
    return exists
  } catch {
    traceLog(`Checked if ${executable} exists with ${command}, errored, returning false`)
    return false
  }
}

/**
 * Get the version of the tcloud bin.
 *
 * @returns The version of the tcloud bin.
 */
async function getTcloudBinVersion(): Promise<Result<[number, number, number], ErrorType>> {
  const tcloudBin = await getTcloudBin()
  if (isErr(tcloudBin)) {
    return tcloudBin
  }
  const called = await execAsync(tcloudBin.value.bin, ['--version'], {
    env: tcloudBin.value.env,
  })
  if (called.exitCode !== 0) {
    return err({
      type: 'generic',
      message: `Failed to get tcloud bin version: ${called.stderr}`,
    })
  }
  const version = called.stdout.split('.').map(Number)
  if (version.length !== 3) {
    return err({
      type: 'generic',
      message: `Failed to get tcloud bin version: ${called.stdout}`,
    })
  }
  return ok(version as [number, number, number])
}