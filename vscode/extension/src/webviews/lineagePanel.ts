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
  } from "vscode"
  
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
    const externalUrl = this.getServerUrl()
    const externalAuthority = new URL(externalUrl).origin;
    
    return `
<!DOCTYPE html>
<html>
<head>
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; frame-src ${externalAuthority}; script-src 'unsafe-inline'; style-src 'unsafe-inline';">
</head>
<body>
  <div>${externalUrl}</div>
  <iframe src="${externalUrl}" style="width:100%; height:100vh;" frameborder="0"></iframe>
</body>
</html>        `
  }


    dispose() {
        // WebviewView doesn't have a dispose method
        // We can clear references
        this.panel = undefined;
    }
  }