export type Callback = {
    openFile: {
        path: string
    }
    formatProject: {}
} 

/**
 * A tuple type representing a callback event with its associated payload.
 * The first element is the callback key (e.g., 'openFile', 'formatProject').
 * The second element is the payload type associated with that key.
 * 
 * Example:
 * const openFileEvent: CallbackEvent<'openFile'> = ['openFile', { path: '/path/to/file' }];
 * const formatProjectEvent: CallbackEvent<'formatProject'> = ['formatProject', {}];
 */
export type CallbackEvent = {
    [K in keyof Callback]: { key: K; payload: Callback[K] }
}[keyof Callback];