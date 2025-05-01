export interface AllModelsMethod {
  method: 'sqlmesh/all_models'
  request: AllModelsRequest
  response: AllModelsResponse
}

// @eslint-disable-next-line  @typescript-eslint/consistent-type-definition
export type CustomLSPMethods = AllModelsMethod

interface AllModelsRequest {
  textDocument: {
    uri: string
  }
}

interface AllModelsResponse {
  models: string[]
  keywords: string[]
}
