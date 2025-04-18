import { AuthenticationProviderTobikoCloud } from "../auth/auth"
import * as vscode from "vscode"


export const signIn = (authenticationProvider: AuthenticationProviderTobikoCloud) => async () => {
    await authenticationProvider.createSession()
    await vscode.window.showInformationMessage("Signed in successfully")
}