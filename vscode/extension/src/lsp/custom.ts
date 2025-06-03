export interface AllModelsMethod {
  method: 'sqlmesh/all_models'
  request: AllModelsRequest
  response: AllModelsResponse
}

export interface RenderModelMethod {
  method: 'sqlmesh/render_model'
  request: RenderModelRequest
  response: RenderModelResponse
}

interface RenderModelRequest {
  textDocumentUri: string
}

interface RenderModelResponse {
  models: RenderModelEntry[]
}

export interface RenderModelEntry {
  name: string
  fqn: string
  description: string | null | undefined
  rendered_query: string
}

// @eslint-disable-next-line  @typescript-eslint/consistent-type-definition
export type CustomLSPMethods =
  | AllModelsMethod
  | AbstractAPICall
  | RenderModelMethod
  | AllModelsForRenderMethod

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

export interface AllModelsForRenderMethod {
  method: 'sqlmesh/all_models_for_render'
  request: AllModelsForRenderRequest
  response: AllModelsForRenderResponse
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
interface AllModelsForRenderRequest {}

interface AllModelsForRenderResponse {
  models: ModelForRendering[]
}

export interface ModelForRendering {
  name: string
  fqn: string
  description: string | null | undefined
  uri: string
}
