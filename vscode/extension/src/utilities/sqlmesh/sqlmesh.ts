import path from "path"
import { traceLog, traceVerbose } from "../common/log"
import { getInterpreterDetails } from "../common/python"
import { Result, err, isErr, ok } from "../functional/result"
import { getProjectRoot } from "../common/utilities"
import { execFile } from "child_process"
import { promisify } from "util"


export type sqlmesh_exec = {
  workspacePath: string;
  bin: string;
  env: Record<string, string | undefined>;
  args: string[];
};

/**
 * Check if tcloud is installed in the current Python environment.
 *
 * @returns A Result indicating whether tcloud is installed.
 */
export const is_tcloud_installed = async (): Promise<
  Result<boolean, string>
> => {
  const interpreterDetails = await getInterpreterDetails()
  if (!interpreterDetails.path) {
    return err("No Python interpreter found")
  }
  traceVerbose(
    `Using interpreter from Python extension: ${interpreterDetails.path.join(
      " "
    )}`
  )

  const pythonPath = interpreterDetails.path[0]
  const checkScript = `
import sys
if sys.version_info >= (3, 12):
    from importlib import metadata
else:
    import importlib_metadata as metadata

try:
    metadata.version('tcloud')
    print("true")
except metadata.PackageNotFoundError:
    print("false")
`
  try {
    const execFileAsync = promisify(execFile)
    traceVerbose(`Checking tcloud installation with script: ${checkScript}`)
    const { stdout } = await execFileAsync(pythonPath, ["-c", checkScript])
    traceVerbose(`tcloud installation check result: ${stdout.trim()}`)
    return ok(stdout.trim() === "true")
  } catch (error) {
    return err(`Failed to check tcloud installation: ${error}`)
  }
}

/**
 * Get the tcloud executable for the current Python environment.
 *
 * @returns The tcloud executable for the current Python environment.
 */
export const get_tcloud_bin = async (): Promise<Result<string, string>> => {
  const interpreterDetails = await getInterpreterDetails()
  if (!interpreterDetails.path) {
    return err("No Python interpreter found")
  }
  const pythonPath = interpreterDetails.path[0]
  const binPath = path.join(path.dirname(pythonPath), "tcloud")
  return ok(binPath)
}

/**
 * Get the sqlmesh executable for the current workspace.
 *
 * @returns The sqlmesh executable for the current workspace.
 */
export const sqlmesh_exec = async (): Promise<Result<sqlmesh_exec, string>> => {
  const projectRoot = await getProjectRoot()
  const workspacePath = projectRoot.uri.fsPath
  const interpreterDetails = await getInterpreterDetails()
  traceLog(`Interpreter details: ${JSON.stringify(interpreterDetails)}`)
  if (interpreterDetails.path) {
    traceVerbose(
      `Using interpreter from Python extension: ${interpreterDetails.path.join(
        " "
      )}`
    )
  }
  if (interpreterDetails.isVirtualEnvironment) {
    traceLog("Using virtual environment")
    const tcloudInstalled = await is_tcloud_installed()
    if (isErr(tcloudInstalled)) {
      return tcloudInstalled
    }
    if (tcloudInstalled.value) {
      const tcloudBin = await get_tcloud_bin()
      if (isErr(tcloudBin)) {
        return tcloudBin
      }
      return ok({
        bin: `${tcloudBin.value} sqlmesh`,
        workspacePath,
        env: {
          PYTHONPATH: interpreterDetails.path?.[0],
          VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
          PATH: interpreterDetails.binPath!,
        },
        args:[], 
      })
    }
    const binPath = path.join(interpreterDetails.binPath!, "sqlmesh")
    traceLog(`Bin path: ${binPath}`)
    return ok({
      bin: binPath,
      workspacePath,
      env: {
        PYTHONPATH: interpreterDetails.path?.[0],
        VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
        PATH: interpreterDetails.binPath!,
      },
      args: [],
    })
  } else {
    return ok({
      bin: "sqlmesh",
      workspacePath,
      env: {},
      args: [],
    })
  }
}

/**
 * Get the sqlmesh_lsp executable for the current workspace.
 *
 * @returns The sqlmesh_lsp executable for the current workspace.
 */
export const sqlmesh_lsp_exec = async (): Promise<
  Result<sqlmesh_exec, string>
> => {
  const projectRoot = await getProjectRoot()
  const workspacePath = projectRoot.uri.fsPath
  const interpreterDetails = await getInterpreterDetails()
  traceLog(`Interpreter details: ${JSON.stringify(interpreterDetails)}`)
  if (interpreterDetails.path) {
    traceVerbose(
      `Using interpreter from Python extension: ${interpreterDetails.path.join(
        " "
      )}`
    )
  }
  if (interpreterDetails.isVirtualEnvironment) {
    traceLog("Using virtual environment")
    const tcloudInstalled = await is_tcloud_installed()
    if (isErr(tcloudInstalled)) {
      return tcloudInstalled
    }
    if (tcloudInstalled.value) {
      traceLog("Tcloud installed, installing sqlmesh")
      const tcloudBin = await get_tcloud_bin()
      if (isErr(tcloudBin)) {
        return tcloudBin
      }
      const execFileAsync = promisify(execFile)
      await execFileAsync(tcloudBin.value, ["install_sqlmesh"], {
        cwd: workspacePath,
        env: {
            PYTHONPATH: interpreterDetails.path?.[0],
            VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
            PATH: interpreterDetails.binPath!,
        },
      })
    }
    const binPath = path.join(interpreterDetails.binPath!, "sqlmesh_lsp")
    traceLog(`Bin path: ${binPath}`)
    return ok({
      bin: binPath,
      workspacePath,
      env: {
        PYTHONPATH: interpreterDetails.path?.[0],
        VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
        PATH: path.join(path.dirname(interpreterDetails.binPath!), "bin"),
      },
      args: [],
    })
  } else {
    return ok({
      bin: "sqlmesh_lsp",
      workspacePath,
      env: {},
      args: [],
    })
  }
}
