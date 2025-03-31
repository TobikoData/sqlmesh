// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import { actual_callout } from './services';
import { createOutputChannel } from './common/vscodeapi';
import { registerLogger, traceInfo } from './common/log';

// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
	traceInfo("Activating extension");

	// Setup logging
	const outputChannel = createOutputChannel("sqlmesh");
	context.subscriptions.push(outputChannel, registerLogger(outputChannel));

	context.subscriptions.push(outputChannel, registerLogger(outputChannel));

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


	// Use the console to output diagnostic information (console.log) and errors (console.error)
	// This line of code will only be executed once when your extension is activated
	traceInfo('Congratulations, your extension "vscode" is now active!');

	// The command has been defined in the package.json file
	// Now provide the implementation of the command with registerCommand
	// The commandId parameter must match the command field in package.json
	const disposable = vscode.commands.registerCommand('sqlmesh.format', () => {
		// The code you place here will be executed every time your command is executed
		// Display a message box to the user
		const out = actual_callout.format()
		if (out === 0) {
			vscode.window.showInformationMessage('SQLMesh format completed successfully');
		} else {
			vscode.window.showErrorMessage('SQLMesh format failed');
		}
	});

	context.subscriptions.push(disposable);

	traceInfo("Extension activated");
}

// This method is called when your extension is deactivated
export function deactivate() {}
