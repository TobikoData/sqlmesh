import type { Callback } from "@bus/callbacks";

export const useVSCode = (): <K extends keyof Callback>(callbackName: K, payload: Callback[K]) => void => {
    return (callbackName, payload) => {
        // Check if we're in an iframe, if so, we are in a webview
        const isIframe = window !== window.parent;
    
        if (isIframe) {
            // Use a different variable name to avoid conflict with the parameter
            const eventPayload = {
                key: callbackName,
                payload: payload,
            }
            window.parent.postMessage({
                key: "vscode_callback",
                payload: eventPayload,
            }, '*');
        } else  {
            console.log("vscode callback", callbackName, payload);
        }
    }
}