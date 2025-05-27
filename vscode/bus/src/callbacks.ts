import type { Result } from './result'

export type CallbackShape = Record<string, any>

export type Callback = {
  openFile: {
    uri: string
  }
  rpcResponse: RPCResponse
} & CallbackShape

/**
 * A tuple type representing a callback event with its associated payload.
 * The first element is the callback key (e.g., 'openFile', 'formatProject').
 * The second element is the payload type associated with that key.
 *
 * Example:
 * const openFileEvent: CallbackEvent<'openFile'> = ['openFile', { path: '/path/to/file' }];
 */
export type CallbackEvent = {
  [K in keyof Callback]: { key: K; payload: Callback[K] }
}[keyof Callback]

export type VSCodeCallbackShape = Record<string, any>

/**
 * A tuple type representing a VSCode event with its associated payload.
 */
export type VSCodeCallback = {
  changeFocusOnFile: {
    path: string
  }
  savedFile: {
    fileUri: string
  }
  rpcRequest: RPCRequest
} & VSCodeCallbackShape

export type VSCodeEvent = {
  [K in keyof VSCodeCallback]: { key: K; payload: VSCodeCallback[K] }
}[keyof VSCodeCallback]

type RPCMethodsShape = Record<string, { params: any; result: any }>

export type RPCMethods = {
  get_active_file: {
    params: {}
    result: {
      fileUri?: string
    }
  }
  api_query: {
    params: {
      url: string
      method: string
      params: any
      body: any
    }
    result: any
  }
} & RPCMethodsShape

export type RPCRequest = {
  requestId: string
  method: keyof RPCMethods
  params: RPCMethods[keyof RPCMethods]['params']
}

export type RPCResponse = {
  requestId: string
  result: Result<RPCMethods[keyof RPCMethods]['result'], string>
}
