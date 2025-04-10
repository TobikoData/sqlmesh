import {
  CancellationToken,
  commands,
  Disposable,
  TextEditor,
  Uri,
  WebviewView,
  WebviewViewProvider,
  WebviewViewResolveContext,
  window,
  workspace,
} from "vscode";
import { isProduction } from "../is_dev";
import { type CallbackEvent } from "@bus/callbacks";
import { getWorkspaceFolders } from "../common/vscodeapi";

export class LineagePanel implements WebviewViewProvider, Disposable {
  public static readonly viewType = "sqlmesh.lineage";

  private panel: WebviewView | undefined;
  private getServerUrl: () => string;
  private _extensionUri: Uri;

  public constructor(extensionUri: Uri, getServerUrl: () => string) {
    this._extensionUri = extensionUri;
    this.getServerUrl = getServerUrl;
    window.onDidChangeActiveTextEditor((event: TextEditor | undefined) => {
      if (this.panel) {
        const { externalUrl, externalAuthority } =
          this.externalUrlAndAutority();
        this.panel.webview.html = this.getHtml(externalUrl, externalAuthority);
      }
    });
  }

  private getPanel() {
    return this.panel;
  }

  public resolveWebviewView(
    webviewView: WebviewView,
    _context: WebviewViewResolveContext,
    _token: CancellationToken
  ) {
    if (this.panel) {
      webviewView = this.panel;
    }
    this.panel = webviewView;

    webviewView.webview.options = {
      // Allow scripts in the webview
      enableScripts: true,
      localResourceRoots: [this._extensionUri],
    };

    // Set content options for external URL access
    // Set up message listener for events from the iframe
    webviewView.webview.onDidReceiveMessage(
      async (message) => {
        console.log("message received", message);
        if (message && message.key) {
          if (message.key === "vscode_callback") {
            const payload: CallbackEvent = message.payload;
            // Handle callback events from the iframe
            switch (payload.key) {
              case "openFile":
                console.log("opening file ", payload.payload.path);
                const workspaceFolders = getWorkspaceFolders();
                if (workspaceFolders.length != 1) {
                  throw new Error("Only one workspace folder is supported");
                }
                const workspaceFolder = workspaceFolders[0];
                const fullPath = Uri.joinPath(
                  workspaceFolder.uri,
                  payload.payload.path
                );
                console.log("fullPath", fullPath);
                const document = await workspace.openTextDocument(fullPath);
                await window.showTextDocument(document);
                break;
              case "formatProject":
                console.log("formatProject", message.payload);
                break;
              default:
                console.log(`Unhandled message type in key: ${message.key}`);
            }
          } else {
            console.log("Unhandled message type: ", message);
          }
        }
      },
      undefined,
      []
    );
    const { externalUrl, externalAuthority } = this.externalUrlAndAutority();
    webviewView.webview.html = this.getHtml(externalUrl, externalAuthority);
  }

  externalUrlAndAutority(): { externalUrl: string; externalAuthority: string } {
    const isProd = isProduction();
    const externalUrl = !isProd
      ? "http://localhost:5173/lineage"
      : this.getServerUrl() + "/lineage";
    const externalAuthority = new URL(externalUrl).origin;
    return { externalUrl, externalAuthority };
  }

  getHtml(externalUrl: string, externalAuthority: string) {
    // The CSP is too restrictive - it only allows frame-src but no other resources
    // Adding connect-src for API calls, img-src for images, and style-src for CSS
    return `
<!DOCTYPE html>
<html>
<head>
  <script>
    // Listen for messages from the iframe and forward them to the extension
    window.addEventListener('message', (event) => {
      // Forward messages from the iframe to the extension
      const message = event.data;
      if (message && message.key) {
        // Post the message to the extension host
        vscode.postMessage(message);
      }
    });
    
    // Define a function to handle events from the extension and send them to the iframe
    const vscode = acquireVsCodeApi();
    window.addEventListener('message', (event) => {
      // Check if the message is from the extension
      if (event.source === window && event.data) {
        // Forward the message to the iframe
        const iframe = document.querySelector('iframe');
        if (iframe && iframe.contentWindow) {
          iframe.contentWindow.postMessage(event.data, '*');
        }
      }
    });
  </script>
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; frame-src ${externalAuthority}; connect-src ${externalAuthority}; img-src ${externalAuthority} data:; script-src 'unsafe-inline' ${externalAuthority}; style-src 'unsafe-inline' ${externalAuthority};">
</head>
<body>
  <iframe src="${externalUrl}" style="width:100%; height:100vh;" frameborder="0" allow="clipboard-read; clipboard-write"></iframe>
</body>
</html>        `;
  }

  dispose() {
    // WebviewView doesn't have a dispose method
    // We can clear references
    this.panel = undefined;
  }
}
