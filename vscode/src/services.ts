import path from "path";
import { traceError, traceLog, traceVerbose } from "./common/log";
import { getInterpreterDetails } from "./common/python";
import { getWorkspaceFolders } from "./common/vscodeapi";
import { execSync } from 'child_process';

interface CLICallout {
    format: () => Promise<number>
}

export const actual_callout: CLICallout = {
    format: async () => {
        traceLog("Calling format")
        try {
            const workspaceFolders = getWorkspaceFolders();
            if (workspaceFolders.length !== 1) {
                traceError("Invalid number of workspace folders");
                return 1;
            }
            const workspacePath = workspaceFolders[0].uri.fsPath;
            const interpreterDetails = await getInterpreterDetails();
            traceLog(`Interpreter details: ${JSON.stringify(interpreterDetails)}`);
            if (interpreterDetails.path) {
                traceVerbose(`Using interpreter from Python extension: ${interpreterDetails.path.join(' ')}`);
            }
            if (interpreterDetails.isVirtualEnvironment) {
                traceLog('Using virtual environment');
                const binPath = path.join(interpreterDetails.binPath!, 'sqlmesh');
                traceLog(`Bin path: ${binPath}`);
                execSync(`${binPath} format`, { encoding: 'utf-8', cwd: workspacePath, 
                    env: {
                        PYTHONPATH: interpreterDetails.path?.[0],
                        VIRTUAL_ENV: path.dirname(interpreterDetails.binPath!),
                        PATH: path.join(path.dirname(interpreterDetails.binPath!), 'bin')
                    }
                 });
                return 0;
            } else {
                return execSync(`sqlmesh format`, { encoding: 'utf-8', cwd: workspacePath });
            }

        } catch (error: any) {
            traceError('Error executing sqlmesh format:', error.message);
            traceError(error.stdout);
            traceError(error.stderr);
            return error.status || 1;
        }
    }
}
