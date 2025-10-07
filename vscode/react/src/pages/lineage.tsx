import {
  QueryCache,
  QueryClient,
  QueryClientProvider,
  useQueryClient,
} from '@tanstack/react-query'
import { useApiModelLineage, useApiModels } from '@/api'
import React, { useState } from 'react'
import { ModelSQLMeshModel } from '@/domain/sqlmesh-model'
import { useEventBus } from '@/hooks/eventBus'
import type { VSCodeEvent } from '@bus/callbacks'
import { URI } from 'vscode-uri'
import type { Model } from '@/api/client'
import { useRpc } from '@/utils/rpc'
import {
  type ModelPath,
  type ModelFullPath,
  type ModelName,
  type ModelEncodedFQN,
  type ModelFQN,
} from '@/domain/models'

import { ModelLineage } from './ModelLineage'
import type {
  ModelLineageNodeDetails,
  ColumnName,
  BrandedLineageAdjacencyList,
  BrandedLineageDetails,
} from './ModelLineageContext'
import type { Column } from '@tobikodata/sqlmesh-common/lineage'
import { useVSCode } from '@/hooks/vscode'
import type { BrandedRecord, BrandedString } from '@bus/brand'

export function LineagePage() {
  const { emit } = useEventBus()

  // Handle messages from VSCode extension
  React.useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      // Ensure the message is from VSCode
      if (event.data && event.data.key === 'vscode_send') {
        const payload: VSCodeEvent = event.data.payload
        switch (payload.key) {
          case 'changeFocusOnFile':
            emit('changeFocusedFile', { fileUri: payload.payload.path })
            break
          case 'savedFile':
            emit('savedFile', { fileUri: payload.payload.fileUri })
            break
          default:
            console.error(
              'Unhandled message type in lineage page:',
              payload.key,
            )
        }
      }
    }
    window.addEventListener('message', handleMessage)
    return () => {
      window.removeEventListener('message', handleMessage)
    }
  }, [])

  const client = new QueryClient({
    queryCache: new QueryCache({}),
    defaultOptions: {
      queries: {
        networkMode: 'always',
        refetchOnWindowFocus: false,
        retry: false,
        staleTime: Infinity,
      },
    },
  })

  return (
    <QueryClientProvider client={client}>
      <Lineage />
    </QueryClientProvider>
  )
}

function Lineage() {
  const [selectedModel, setSelectedModel] = useState<ModelFQN | undefined>(
    undefined,
  )
  const { on } = useEventBus()
  const queryClient = useQueryClient()

  const {
    data: models,
    isLoading: isLoadingModels,
    error: modelsError,
  } = useApiModels()
  const rpc = useRpc()
  React.useEffect(() => {
    const fetchFirstTimeModelIfNotSet = async <T extends BrandedString>(
      models: Model[],
    ): Promise<T | undefined> => {
      if (!Array.isArray(models)) {
        return undefined
      }
      const activeFile = await rpc('get_active_file', {})
      // @ts-ignore
      if (!activeFile.fileUri) {
        return models[0].fqn as T
      }
      // @ts-ignore
      const fileUri: string = activeFile.fileUri
      const filePath = URI.file(fileUri).path
      const model = models.find((m: Model) => {
        if (!m.full_path) {
          return false
        }
        return URI.file(m.full_path).path === filePath
      })
      if (model) {
        return model.fqn as T
      }
      return undefined
    }
    if (selectedModel === undefined && Array.isArray(models)) {
      fetchFirstTimeModelIfNotSet<ModelFQN>(models).then(modelName => {
        if (modelName && selectedModel === undefined) {
          setSelectedModel(modelName)
        } else {
          setSelectedModel(models[0].fqn as ModelFQN)
        }
      })
    }
  }, [models, selectedModel])

  const modelsRecord =
    Array.isArray(models) &&
    models.reduce(
      (acc, model) => {
        acc[model.fqn as ModelFQN] = model
        return acc
      },
      {} as BrandedRecord<ModelFQN, Model>,
    )

  React.useEffect(() => {
    const handleChangeFocusedFile = (fileUri: { fileUri: string }) => {
      const full_path = URI.parse(fileUri.fileUri).path
      const model: Model | undefined = Object.values(modelsRecord).find(
        m => URI.file(m.full_path).path === full_path,
      )
      if (model) {
        setSelectedModel(model.fqn as ModelFQN)
      }
    }

    const handleSavedFile = () => {
      queryClient.invalidateQueries()
    }

    const offChangeFocusedFile = on(
      'changeFocusedFile',
      handleChangeFocusedFile,
    )
    const offSavedFile = on('savedFile', handleSavedFile)

    // If your event bus returns an "off" function, call it on cleanup
    return () => {
      if (offChangeFocusedFile) offChangeFocusedFile()
      if (offSavedFile) offSavedFile()
    }
  }, [on, queryClient, modelsRecord])

  if (modelsError) {
    return <div>Error: {modelsError.message}</div>
  }

  if (
    isLoadingModels ||
    models === undefined ||
    modelsRecord === false ||
    selectedModel === undefined
  ) {
    return <div>Loading models...</div>
  }
  if (!Array.isArray(models)) {
    return <div>Error: Models data is not in the expected format</div>
  }

  return (
    <LineageComponentFromWeb
      selectedModel={selectedModel}
      models={modelsRecord}
    />
  )
}

export function LineageComponentFromWeb({
  selectedModel,
  models,
}: {
  selectedModel: ModelFQN
  models: BrandedRecord<ModelFQN, Model>
}) {
  const vscode = useVSCode()
  function handleClickModel(id: string): void {
    const decodedId = decodeURIComponent(id)
    const model = (Object.values(models) as Model[]).find(
      m => m.fqn === decodedId,
    )
    if (!model) {
      throw new Error('Model not found')
    }
    if (!model.full_path) {
      return
    }
    vscode('openFile', { uri: URI.file(model.full_path).toString() })
  }

  function handleError(error: any): void {
    console.error(error)
  }

  const model = models[selectedModel]
  if (!model) {
    return <div>Error: Model not found</div>
  }

  const sqlmModel = new ModelSQLMeshModel()
  sqlmModel.update({
    ...model,
    name: model.name as ModelName,
    fqn: model.fqn as ModelEncodedFQN,
    path: model.path as ModelPath,
    full_path: model.full_path as ModelFullPath,
  })

  const { refetch: getModelLineage } = useApiModelLineage(model?.name ?? '')

  const [modelLineage, setModelLineage] = useState<
    BrandedLineageAdjacencyList<ModelFQN> | undefined
  >(undefined)

  React.useEffect(() => {
    if (model === undefined) return

    getModelLineage()
      .then(({ data }) => {
        setModelLineage(
          data as unknown as BrandedLineageAdjacencyList<ModelFQN>,
        )
      })
      .catch(handleError)
  }, [model?.name, model?.hash])

  const lineageDetails = (Object.values(models) as Model[]).reduce(
    (acc, model) => {
      const modelFQN = model.fqn as ModelFQN
      acc[modelFQN] = {
        name: modelFQN,
        display_name: model.name as ModelName,
        model_type: model.type,
        identifier: undefined,
        version: undefined,
        dialect: model.dialect,
        cron: model.details?.cron,
        owner: model.details?.owner,
        kind: model.details?.kind,
        tags: [],
        columns: model.columns.reduce(
          (acc, column) => {
            const columnName = decodeURI(column.name) as ColumnName
            acc[columnName] = {
              data_type: column.type,
              description: column.description,
            }
            return acc
          },
          {} as BrandedRecord<ColumnName, Column>,
        ),
      }
      return acc
    },
    {} as BrandedLineageDetails<ModelFQN, ModelLineageNodeDetails>,
  )

  if (!modelLineage || !lineageDetails) {
    return null
  }

  return (
    <ModelLineage
      className="h-[98vh] w-[97vw]"
      onNodeClick={(_, node) => handleClickModel(node.id)}
      selectedModelName={model.fqn as ModelFQN}
      adjacencyList={modelLineage}
      lineageDetails={lineageDetails}
    />
  )
}
