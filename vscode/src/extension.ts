// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import { actual_callout } from './services';
import { createOutputChannel } from './common/vscodeapi';
import { registerLogger, traceInfo, traceLog, traceVerbose } from './common/log';
import { getInterpreterDetails } from './common/python';

// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
	const outputChannel = createOutputChannel("sqlmesh");
	context.subscriptions.push(outputChannel, registerLogger(outputChannel));

	traceInfo("Activating extension");

	const changeLogLevel = async (c: vscode.LogLevel, g: vscode.LogLevel) => {
		// TODO: Implement this
        // const level = getLSClientTraceLevel(c, g);
        // await lsClient?.setTrace(level);
    };

    context.subscriptions.push(
        outputChannel.onDidChangeLogLevel(async (e) => {
            await changeLogLevel(e, vscode.env.logLevel);
        }),
        vscode.env.onDidChangeLogLevel(async (e) => {
            await changeLogLevel(outputChannel.logLevel, e);
        }),
    );


	// The command has been defined in the package.json file
	// Now provide the implementation of the command with registerCommand
	// The commandId parameter must match the command field in package.json
	const disposable = vscode.commands.registerCommand('sqlmesh.format', async () => {
		const out = await actual_callout.format();
		if (out === 0) {
			vscode.window.showInformationMessage('SQLMesh format completed successfully');
		} else {
			vscode.window.showErrorMessage('SQLMesh format failed');
		}
	});

	context.subscriptions.push(disposable);

	setImmediate(async () => {
		const interpreterDetails = await getInterpreterDetails();
		if (interpreterDetails.path) {
			traceLog(`Using interpreter from Python extension: ${interpreterDetails.path.join(' ')}`);
		}
	});
	traceInfo("Extension activated");
}

// This method is called when your extension is deactivated
export function deactivate() {}
