import path from "path"
import { traceLog, traceVerbose } from "../common/log"
import { getInterpreterDetails } from "../common/python"
import { Result, ok } from "../functional/result"
import { getProjectRoot } from "../common/utilities"

export type sqlmesh_exec = {
    workspacePath: string;
    bin: string;
    env: Record<string, string | undefined>;
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
        traceVerbose(`Using interpreter from Python extension: ${interpreterDetails.path.join(' ')}`)
    }
    if (interpreterDetails.isVirtualEnvironment) {
        traceLog('Using virtual environment')
        const binPath = path.join(interpreterDetails.binPath!, 'sqlmesh')
        traceLog(`Bin path: ${binPath}`)
        return ok({
            bin: binPath,
            workspacePath,
            env: {
                PYTHONPATH: interpreterDetails.path?.[0],
                VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
                PATH: path.join(path.dirname(interpreterDetails.binPath!), 'bin')
            }
         })
    } else {
        return ok({
            bin: 'sqlmesh',
            workspacePath,
            env: {},
        })
    }
}

/**
 * Get the sqlmesh_lsp executable for the current workspace.
 * 
 * @returns The sqlmesh_lsp executable for the current workspace.
 */
export const sqlmesh_lsp_exec = async (): Promise<Result<sqlmesh_exec, string>> => {
    const projectRoot = await getProjectRoot()
    const workspacePath = projectRoot.uri.fsPath
    const interpreterDetails = await getInterpreterDetails()
    traceLog(`Interpreter details: ${JSON.stringify(interpreterDetails)}`)
    if (interpreterDetails.path) {
        traceVerbose(`Using interpreter from Python extension: ${interpreterDetails.path.join(' ')}`)
    }
    if (interpreterDetails.isVirtualEnvironment) {
        traceLog('Using virtual environment')
        const binPath = path.join(interpreterDetails.binPath!, 'sqlmesh_lsp')
        traceLog(`Bin path: ${binPath}`)
        return ok({
            bin: binPath,
            workspacePath,
            env: {
                PYTHONPATH: interpreterDetails.path?.[0],
                VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
                PATH: path.join(path.dirname(interpreterDetails.binPath!), 'bin')
            }
         })
    } else {
        return ok({
            bin: 'sqlmesh_lsp',
            workspacePath,
            env: {},
        })
    }

}
