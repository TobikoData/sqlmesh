import {
  type RPCRequest,
  type RPCMethods,
  type CallbackEvent,
} from '@bus/callbacks'
import { type Result } from '@bus/result'
import { sendVSCodeMessage } from './vscodeapi'

export const useRpc = () => {
  return callRpc
}

export const callRpc = async <T extends keyof RPCMethods>(
  method: T,
  params: RPCMethods[T]['params'],
): Promise<Result<RPCMethods[T]['result'], string>> => {
  return new Promise((resolve, reject) => {
    const requestId = `query_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
    const messageHandler = (event: MessageEvent) => {
      if (event.data) {
        const eventData = event.data as CallbackEvent
        if (eventData.key !== 'rpcResponse') {
          return
        }
        if (eventData.payload.requestId !== requestId) {
          return
        }
        const payload = eventData.payload.result
        window.removeEventListener('message', messageHandler)
        return resolve(payload)
      }
    }

    // Add the listener
    window.addEventListener('message', messageHandler)

    const request: RPCRequest = {
      requestId,
      method,
      params,
    }
    sendVSCodeMessage('rpcRequest', request)

    // Set a timeout to prevent hanging promises
    setTimeout(() => {
      window.removeEventListener('message', messageHandler)
      reject(new Error('Query request timed out'))
    }, 30000) // 30 second timeout
  })
}
