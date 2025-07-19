import '../App.css'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { TableDiff } from '../components/tablediff/TableDiff'

export function TableDiffPage() {
  const client = new QueryClient({
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
      <TableDiff />
    </QueryClientProvider>
  )
}
