import { createFileRoute } from '@tanstack/react-router'
import '../App.css'
import {
  QueryCache,
  QueryClient,
  QueryClientProvider,
} from '@tanstack/react-query'
import { useApiModelLineage } from '@/api'

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
  const { data, isLoading } = useApiModelLineage('sushi.customers')

  return (
    <>
      <div className="text-3xl font-bold underline">Hello from Lineage!</div>
      <div>data: {JSON.stringify(data)}</div>
      <div>isLoading: {JSON.stringify(isLoading)}</div>
    </>
  )
}