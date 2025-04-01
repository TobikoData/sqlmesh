import {
    LogOutputChannel,
    window,
    workspace,
    WorkspaceFolder,
} from 'vscode';


export function createOutputChannel(name: string): LogOutputChannel {
    return window.createOutputChannel(name, { log: true });
}

export function getWorkspaceFolders(): readonly WorkspaceFolder[] {
    return workspace.workspaceFolders ?? [];
}