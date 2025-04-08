import path from "path"
import { traceLog, traceVerbose } from "../common/log"
import { getInterpreterDetails } from "../common/python"
import { getWorkspaceFolders } from "../common/vscodeapi"
import { Result, err, ok } from "../functional/result"

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
    const workspaceFolders = getWorkspaceFolders()
    if (workspaceFolders.length !== 1) {
        return err("Invalid number of workspace folders")
    }
    const workspacePath = workspaceFolders[0].uri.fsPath
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