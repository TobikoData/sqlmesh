import { CallbackEvent } from '@bus/callbacks'
import {
  Disposable,
  Uri,
  Webview,
  WebviewView,
  WebviewViewProvider,
  window,
  workspace,
} from 'vscode'
import { getWorkspaceFolders } from '../utilities/common/vscodeapi'
import { LSPClient } from '../lsp/lsp'

export class LineagePanel implements WebviewViewProvider, Disposable {
  public static readonly viewType = 'sqlmesh.lineage'

  private panel: WebviewView | undefined
  private lsp: LSPClient
  private _extensionUri: Uri

  public constructor(extensionUri: Uri, lsp: LSPClient) {
    this._extensionUri = extensionUri
    this.lsp = lsp

    if (this.panel) {
      this.panel.webview.html = this.getHTML(this.panel.webview)
    }

    workspace.onDidSaveTextDocument(document => {
      this.panel?.webview.postMessage({
        key: 'vscode_send',
        payload: {
          key: 'savedFile',
          payload: { fileUri: document.uri.fsPath },
        },
      })
    })

    window.onDidChangeActiveTextEditor(editor => {
      if (editor) {
        this.panel?.webview.postMessage({
          key: 'vscode_send',
          payload: {
            key: 'changeFocusOnFile',
            payload: { path: editor.document.uri.fsPath },
          },
        })
      }
    })
  }

  public resolveWebviewView(webviewView: WebviewView) {
    if (this.panel) {
      webviewView = this.panel
    }
    this.panel = webviewView

    webviewView.webview.options = {
      // Allow scripts in the webview
      enableScripts: true,
      localResourceRoots: [this._extensionUri],
    }

    // Set content options for external URL access
    // Set up message listener for events from the iframe
    webviewView.webview.onDidReceiveMessage(
      async request => {
        if (!request) {
          return
        }
        if (!request.key) {
          return
        }
        const message: CallbackEvent = request
        switch (message.key) {
          case 'openFile': {
            const workspaceFolders = getWorkspaceFolders()
            if (workspaceFolders.length != 1) {
              throw new Error('Only one workspace folder is supported')
            }
            const fullPath = Uri.parse(message.payload.uri)
            const document = await workspace.openTextDocument(fullPath)
            await window.showTextDocument(document)
            break
          }
          case 'queryRequest': {
            const payload = message.payload
            const requestId = message.payload.requestId
            const response = await this.lsp.call_custom_method(
              'sqlmesh/api',
              payload as any,
            )
            webviewView.webview.postMessage({
              key: 'query_response',
              payload: response,
              requestId,
            })
            break
          }
          default:
            console.error(
              'Unhandled message type under queryRequest: ',
              message,
            )
        }
      },
      undefined,
      [],
    )
    webviewView.webview.html = this.getHTML(webviewView.webview)
  }

  private getHTML(panel: Webview) {
    const cssUri = panel.asWebviewUri(
      Uri.joinPath(this._extensionUri, 'src_react', 'assets', 'index.css'),
    )
    const jsUri = panel.asWebviewUri(
      Uri.joinPath(this._extensionUri, 'src_react', 'assets', 'index.js'),
    )
    const faviconUri = panel.asWebviewUri(
      Uri.joinPath(this._extensionUri, 'src_react', 'favicon.ico'),
    )
    const logoUri = panel.asWebviewUri(
      Uri.joinPath(this._extensionUri, 'src_react', 'logo192.png'),
    )

    // Handle query requests from the React app

    return `
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <link rel="icon" href="${faviconUri}" />
    <meta name="theme-color" content="#000000" />
    <meta
      name="description"
      content="Web site created using create-tsrouter-app"
    />
    <link rel="apple-touch-icon" href="${logoUri}" />
    <title>Create TanStack App - react</title>
    <script type="module" crossorigin src="${jsUri}"></script>
    <link rel="stylesheet" crossorigin href="${cssUri}">
   </head>
  <body>
    <div id="app"></div>
  </body>
</html>
`
  }

  dispose() {
    // WebviewView doesn't have a dispose method
    // We can clear references
    this.panel = undefined
  }
}
