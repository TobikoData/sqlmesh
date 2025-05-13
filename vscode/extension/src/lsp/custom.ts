export interface AllModelsMethod {
  method: 'sqlmesh/all_models'
  request: AllModelsRequest
  response: AllModelsResponse
}

// @eslint-disable-next-line  @typescript-eslint/consistent-type-definition
export type CustomLSPMethods = AllModelsMethod | AbstractAPICall

interface AllModelsRequest {
  textDocument: {
    uri: string
  }
}

interface AllModelsResponse {
  models: string[]
  keywords: string[]
}

export interface AbstractAPICallRequest {
  endpoint: string
  method: string
  params: Record<string, any>
  body: Record<string, any>
}

export interface AbstractAPICall {
  method: 'sqlmesh/api'
  request: AbstractAPICallRequest
  response: object
}
