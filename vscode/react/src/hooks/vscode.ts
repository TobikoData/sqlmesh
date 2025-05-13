import { sendVSCodeMessage } from '@/utils/vscodeapi'

/**
 * use this hook to send messages to the vscode extension
 *
 * when deving the extension, we use an iframe to load the react app
 * so we need to send messages to the parent window
 */
export const useVSCode = () => sendVSCodeMessage
