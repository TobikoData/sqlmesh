import { createFileRoute } from '@tanstack/react-router'
import '../App.css'
import {
  QueryCache,
  QueryClient,
  QueryClientProvider,
} from '@tanstack/react-query'
import { useApiModelLineage, useApiModels } from '@/api'
import LineageFlowProvider from '@/components/graph/context'
import { ModelLineage } from '@/components/graph/ModelLineage'
import { useVSCode } from '@/hooks/vscode'

export const Route = createFileRoute('/lineage')({
  component: Wrappper,
})

function Wrappper() {
  const client = new QueryClient({
    queryCache: new QueryCache({
      onError(error, query) {
        console.error(error, query)
      },
      onSuccess(data, query) {
        console.log('success', data, query)
      },
    }),
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
  const selectedModel = 'sushi.customers'

  const { data, isLoading } = useApiModelLineage(selectedModel)
  const { data: models, isLoading: isLoadingModels } = useApiModels()
  if (isLoading) {
    return <div>Loading...</div>
  }
  if (isLoadingModels) {
    return <div>Loading models...</div>
  }
  console.log('models', models)
  console.log('data', data)
  const modelsRecord = models?.reduce((acc, model) => {
    acc[model.name] = model
    return acc
  }, {} as Record<string, Model>)

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
  selectedModel: string,
  models: Record<string, Model>;
}): JSX.Element {
  const vscode = useVSCode()
  function handleClickModel(id: string): void {
    const decodedId = decodeURIComponent(id);
    const model = Object.values(models).find((m: Model) => m.fqn === decodedId);
    if (!model) {
      throw new Error('Model not found');
    }
    vscode('openFile', { path: model.path });
  }

  function handleError(error: any): void {
    console.log(error)
  }

  console.log('models inside', models)
  const model = models[selectedModel]

  return (
    <div className="h-[100vh] w-[100vw]">
      <LineageFlowProvider
        showColumns={true}
        handleClickModel={handleClickModel}
        handleError={handleError}
        models={models}
        showControls={false}
      >
        <ModelLineage model={model} />
      </LineageFlowProvider>
    </div>
  )
}
