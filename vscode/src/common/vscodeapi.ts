import {
    LogOutputChannel,
    window,
} from 'vscode';


export function createOutputChannel(name: string): LogOutputChannel {
    return window.createOutputChannel(name, { log: true });
}