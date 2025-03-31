import { traceError, traceLog } from "./common/log";

interface CLICallout {
    format: () => number
}

export const actual_callout: CLICallout = {
    format: () => {
        console.log("calling format")

        const { execSync } = require('child_process');
        try {
            return execSync('sqlmesh format', { encoding: 'utf-8' });
        } catch (error: any) {
            traceError('Error executing sqlmesh format:', error.message);
            traceError(error.stdout);
            traceError(error.stderr);
            return error.status || 1;
        }
    }
}