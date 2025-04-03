// The module 'vscode' contains the VS Code extensibility API
// Import the module and reference it with the alias vscode in your code below
import * as vscode from 'vscode';
import { actual_callout } from './services';
import { createOutputChannel, onDidChangeConfiguration, registerCommand } from './common/vscodeapi';
import { registerLogger, traceError, traceInfo, traceLog, traceVerbose } from './common/log';
import { checkVersion, getInterpreterDetails, onDidChangePythonInterpreter, resolveInterpreter } from './common/python';
import { restartServer } from './common/server';
import { LanguageClient } from 'vscode-languageclient/lib/node/main';
import { getLSClientTraceLevel } from './common/utilities';
import { loadServerDefaults } from './common/setup';
import { checkIfConfigurationChanged } from './common/settings';
import { getInterpreterFromSetting } from './common/settings';
import { startWebServer, WebServer } from './web-server/server';
import { isErr } from './functional/result';

let lsClient: LanguageClient | undefined;
let webServer: WebServer | undefined;


// This method is called when your extension is activated
// Your extension is activated the very first time the command is executed
export function activate(context: vscode.ExtensionContext) {
	const extensionOutputChannel = createOutputChannel("sqlmesh");
	context.subscriptions.push(extensionOutputChannel, registerLogger(extensionOutputChannel));
	const lspOutputChannel = createOutputChannel("sqlmesh-lsp");
	context.subscriptions.push(lspOutputChannel, registerLogger(lspOutputChannel));
	const serverOutputChannel = createOutputChannel("sqlmesh-server");
	context.subscriptions.push(serverOutputChannel, registerLogger(serverOutputChannel));

	traceInfo("Activating extension");

	const changeLogLevel = async (c: vscode.LogLevel, g: vscode.LogLevel) => {
        const level = getLSClientTraceLevel(c, g);
        await lsClient?.setTrace(level);
    };

    context.subscriptions.push(
        lspOutputChannel.onDidChangeLogLevel(async (e) => {
            await changeLogLevel(e, vscode.env.logLevel);
        }),
        vscode.env.onDidChangeLogLevel(async (e) => {
            await changeLogLevel(lspOutputChannel.logLevel, e);
        }),
    );


	// The command has been defined in the package.json file
	// Now provide the implementation of the command with registerCommand
	// The commandId parameter must match the command field in package.json
	const disposable = vscode.commands.registerCommand('sqlmesh.format', async () => {
		const out = await actual_callout.format();
		if (out === 0) {
			vscode.window.showInformationMessage('Project formatted successfully');
		} else {
			vscode.window.showErrorMessage('Project format failed');
		}
	});

	context.subscriptions.push(disposable);

    const serverInfo = loadServerDefaults();
    const serverName = serverInfo.name;
    const serverId = serverInfo.module;

	// Log Server information
	traceLog(`Name: ${serverInfo.name}`);
	traceLog(`Module: ${serverInfo.module}`);
	traceVerbose(`Full Server Info: ${JSON.stringify(serverInfo)}`);

	const runServer = async () => {
		const interpreter = getInterpreterFromSetting(serverId);
		if (interpreter && interpreter.length > 0) {
			if (checkVersion(await resolveInterpreter(interpreter))) {
				traceVerbose(`Using interpreter from ${serverInfo.module}.interpreter: ${interpreter.join(' ')}`);
				lsClient = await restartServer(serverId, serverName, lspOutputChannel, lsClient);
			}
			return;
		}

		const interpreterDetails = await getInterpreterDetails();
		if (interpreterDetails.path) {
			traceVerbose(`Using interpreter from Python extension: ${interpreterDetails.path.join(' ')}`);
			lsClient = await restartServer(serverId, serverName, lspOutputChannel, lsClient);
			return;
		}

		traceError(
			'Python interpreter missing:\r\n' +
				'[Option 1] Select python interpreter using the ms-python.python.\r\n' +
				`[Option 2] Set an interpreter using "${serverId}.interpreter" setting.\r\n` +
				'Please use Python 3.8 or greater.',
		);
	};

	context.subscriptions.push(
		onDidChangePythonInterpreter(async () => {
			await runServer();
			await restartUIServer(serverOutputChannel);
		}),
		onDidChangeConfiguration(async (e: vscode.ConfigurationChangeEvent) => {
			if (checkIfConfigurationChanged(e, serverId) {
				await restartUIServer(serverOutputChannel);
				await restartServer(serverId, serverName, lspOutputChannel, lsClient);
			}
		}),
		registerCommand(`${serverId}.restart`, async () => {
			await runServer();
			await restartUIServer(serverOutputChannel);
		}),
	);

	startServer(serverOutputChannel);

	setImmediate(async () => {
		const interpreterDetails = await getInterpreterDetails();
		if (interpreterDetails.path) {
			traceLog(`Using interpreter from Python extension: ${interpreterDetails.path.join(' ')}`);
		}
	});
	traceInfo("Extension activated");
}

export async function startServer(serverOutputChannel: vscode.LogOutputChannel) {
    if (webServer) {
        return webServer;
    }
    const result = await startWebServer(serverOutputChannel);
    if (isErr(result)) {
        throw new Error(result.error);
    }
    webServer = result.value;
    return webServer;
}

export async function restartUIServer(serverOutputChannel: vscode.LogOutputChannel) {
	if (webServer) {
		webServer.stop();
	}
	await startServer(serverOutputChannel);
}


// This method is called when your extension is deactivated
export function deactivate() {
	if (webServer) {
		webServer.stop();
	}
}

