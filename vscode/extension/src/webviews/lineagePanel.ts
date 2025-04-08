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
  } from "vscode"
  import { isProduction } from "../is_dev"

  export class LineagePanel implements WebviewViewProvider, Disposable {
    public static readonly viewType = "sqlmesh.lineage"
  
    private panel: WebviewView | undefined
    private getServerUrl: () => string
    private _extensionUri: Uri
  
    public constructor(		
        extensionUri: Uri,
        getServerUrl: () => string
    ) {
      this._extensionUri = extensionUri
      this.getServerUrl = getServerUrl
      window.onDidChangeActiveTextEditor((event: TextEditor | undefined) => {
        if (this.panel) {
          this.panel.webview.html = this.getHtml()
        }
      })
    }

    private getPanel() {
      return this.panel
    }

    public resolveWebviewView(
		webviewView: WebviewView,
		_context: WebviewViewResolveContext,
		_token: CancellationToken,
	) {
        if (this.panel) {
            webviewView = this.panel
        }
		this.panel = webviewView

		webviewView.webview.options = {
			// Allow scripts in the webview
			enableScripts: true,
			localResourceRoots: [
				this._extensionUri
			]
		}

    // Set content options for external URL access
    const externalUrl = this.getServerUrl();
    const externalAuthority = new URL(externalUrl).origin;
    
    webviewView.webview.html = this.getHtml()
	}

  getHtml() {
    
    const isProd = isProduction()
    
    const externalUrl = !isProd ? "http://localhost:5173" : this.getServerUrl()
    const externalAuthority = new URL(externalUrl).origin;
    
    // The CSP is too restrictive - it only allows frame-src but no other resources
    // Adding connect-src for API calls, img-src for images, and style-src for CSS
    return `
<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; frame-src ${externalAuthority}; connect-src ${externalAuthority}; img-src ${externalAuthority} data:; script-src 'unsafe-inline' ${externalAuthority}; style-src 'unsafe-inline' ${externalAuthority};">
</head>
<body>
  <div>${isProd ? "Production" : "Development"}</div>
  <div>${externalUrl}</div>
  <iframe src="${externalUrl}" style="width:100%; height:100vh;" frameborder="0" allow="clipboard-read; clipboard-write"></iframe>
</body>
</html>        `
  }


    dispose() {
        // WebviewView doesn't have a dispose method
        // We can clear references
        this.panel = undefined;
    }
  }