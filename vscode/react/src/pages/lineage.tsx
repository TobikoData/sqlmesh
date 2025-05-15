import '../App.css'
import {
  QueryCache,
  QueryClient,
  QueryClientProvider,
  useQueryClient,
} from '@tanstack/react-query'
import { useApiModels } from '@/api'
import LineageFlowProvider from '@/components/graph/context'
import { ModelLineage } from '@/components/graph/ModelLineage'
import { useVSCode } from '@/hooks/vscode'
import React, { useState } from 'react'
import { ModelSQLMeshModel } from '@/domain/sqlmesh-model'
import { useEventBus } from '@/hooks/eventBus'
import type { VSCodeEvent } from '@bus/callbacks'

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
  const [selectedModelSet, setSelectedModelSet] = useState<string | undefined>(
    undefined,
  )
  const { on } = useEventBus()
  const queryClient = useQueryClient()

  const { data: models, isLoading: isLoadingModels } = useApiModels()
  React.useEffect(() => {
    if (selectedModelSet === undefined && models && Array.isArray(models)) {
      setSelectedModelSet(models[0].name)
    }
  }, [models, selectedModelSet])

  if (
    isLoadingModels ||
    models === undefined ||
    selectedModelSet === undefined
  ) {
    return <div>Loading models...</div>
  }
  if (!Array.isArray(models)) {
    return <div>Error: Models data is not in the expected format</div>
  }
  const modelsRecord = models.reduce(
    (acc, model) => {
      // @ts-ignore
      acc[model.name] = model
      return acc
    },
    {} as Record<string, Model>,
  )
  const selectedModel = selectedModelSet
  on('changeFocusedFile', fileUri => {
    console.log('on changeFocusedFile', fileUri)
    const full_path = fileUri.fileUri.startsWith('file://')
      ? fileUri.fileUri.substring(7)
      : fileUri.fileUri
    const model = Object.values(modelsRecord).find(
      (m: Model) => m.full_path === full_path,
    )
    if (model) {
      setSelectedModelSet(model.name)
    }
  })
  on('savedFile', () => {
    queryClient.invalidateQueries()
  })

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
  selectedModel: string
  models: Record<string, Model>
}): JSX.Element {
  const vscode = useVSCode()
  function handleClickModel(id: string): void {
    console.log('handling click', id)
    const decodedId = decodeURIComponent(id)
    const model = Object.values(models).find((m: Model) => m.fqn === decodedId)
    if (!model) {
      throw new Error('Model not found')
    }
    vscode('openFile', { uri: 'file://' + model.full_path })
  }

  function handleError(error: any): void {
    console.log(error)
  }

  const model = models[selectedModel]
  if (!model) {
    return <div>Error: Model not found</div>
  }

  const sqlmModel = new ModelSQLMeshModel()
  sqlmModel.update(model)

  return (
    <div className="h-[100vh] w-[100vw]">
      <LineageFlowProvider
        showColumns={true}
        handleClickModel={handleClickModel}
        handleError={handleError}
        models={models}
        showControls={false}
      >
        <ModelLineage model={sqlmModel} />
      </LineageFlowProvider>
    </div>
  )
}
