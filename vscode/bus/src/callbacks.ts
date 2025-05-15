export type Callback = {
  openFile: {
    uri: string
  }
  queryRequest: {
    requestId: string
    url: string
    method?: string
    params?: Record<string, string>
    body?: Record<string, any>
  }
}

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

/**
 * A tuple type representing a VSCode event with its associated payload.
 */
export type VSCodeCallback = {
  changeFocusOnFile: {
    path: string
  }
  queryResponse: {
    requestId: string
    response: any
  }
  savedFile: {
    fileUri: string
  }
}

export type VSCodeEvent = {
  [K in keyof VSCodeCallback]: { key: K; payload: VSCodeCallback[K] }
}[keyof VSCodeCallback]
