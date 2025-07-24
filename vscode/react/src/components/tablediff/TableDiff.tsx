import { useState, useEffect } from 'react'
import LoadingStatus from '../loading/LoadingStatus'
import { TableDiffResults } from './TableDiffResults'
import { callRpc } from '../../utils/rpc'
import { type TableDiffData } from './types'

interface ModelInfo {
  name: string
  fqn: string
  description?: string
}

export function TableDiff() {
  const [selectedModel, setSelectedModel] = useState<ModelInfo | null>(null)
  const [sourceEnvironment, setSourceEnvironment] = useState<string>('prod')
  const [targetEnvironment, setTargetEnvironment] = useState<string>('dev')
  const [tableDiffData, setTableDiffData] = useState<TableDiffData | null>(null)
  const [isLoadingDiff] = useState(false)
  const [diffError] = useState<string | null>(null)
  const [hasInitialData, setHasInitialData] = useState(false)

  const handleDataUpdate = (newData: TableDiffData) => {
    setTableDiffData(newData)
  }

  // Load initial data on mount
  useEffect(() => {
    const loadInitialData = async () => {
      try {
        // Try to get initial data first (pre-selected from VSCode)
        const initialDataResult = await callRpc('get_initial_data', {})
        if (initialDataResult.ok && initialDataResult.value) {
          const data = initialDataResult.value

          // Set all initial state from pre-selected data
          if (data.selectedModel) {
            setSelectedModel(data.selectedModel)
          }
          if (data.sourceEnvironment) {
            setSourceEnvironment(data.sourceEnvironment)
          }
          if (data.targetEnvironment) {
            setTargetEnvironment(data.targetEnvironment)
          }

          // Always mark as having initial data if we got a response from VSCode
          setHasInitialData(true)

          if (data.tableDiffData) {
            // Handle different response structures
            let diffData: TableDiffData | null = null

            if (data.tableDiffData.data !== undefined) {
              // Response has a nested data field
              diffData = data.tableDiffData.data
            } else if (
              data.tableDiffData &&
              typeof data.tableDiffData === 'object' &&
              'schema_diff' in data.tableDiffData &&
              'row_diff' in data.tableDiffData
            ) {
              // Response is the data directly
              diffData = data.tableDiffData as TableDiffData
            }

            setTableDiffData(diffData)
          }
        }
      } catch (error) {
        console.error('Error loading initial data:', error)
      }
    }

    loadInitialData()
  }, [])

  // If we're still loading, show loading state
  if (isLoadingDiff) {
    return (
      <div className="h-[100vh] w-[100vw]">
        <LoadingStatus>Running table diff...</LoadingStatus>
      </div>
    )
  }

  // If we have initial data, handle all possible states
  if (hasInitialData) {
    // Show results if we have them
    if (tableDiffData) {
      return (
        <div className="h-[100vh] w-[100vw]">
          <TableDiffResults
            data={tableDiffData}
            onDataUpdate={handleDataUpdate}
          />
        </div>
      )
    }

    // Show error if there was one
    if (diffError) {
      return (
        <div className="h-[100vh] w-[100vw] flex items-center justify-center">
          <div className="text-red-400 text-center">
            <div className="text-lg font-semibold mb-2">
              Error running table diff
            </div>
            <div>{diffError}</div>
          </div>
        </div>
      )
    }

    // If we have initial data but no results and no error, show appropriate message
    return (
      <div className="h-[100vh] w-[100vw] flex items-center justify-center">
        <div className="text-neutral-400 text-center">
          <div className="text-lg font-semibold mb-2">No differences found</div>
          <div>
            The selected model "{selectedModel?.name}" has no differences
            between <span className="text-blue-400">{sourceEnvironment}</span>{' '}
            and <span className="text-green-400">{targetEnvironment}</span>{' '}
            environments.
          </div>
        </div>
      </div>
    )
  }

  // If we don't have initial data yet, show loading
  if (!hasInitialData) {
    return (
      <div className="h-[100vh] w-[100vw]">
        <LoadingStatus>Loading...</LoadingStatus>
      </div>
    )
  }

  // This should never happen with the new flow
  return (
    <div className="h-[100vh] w-[100vw] flex items-center justify-center">
      <div className="text-neutral-400 text-center">
        <div className="text-lg font-semibold mb-2">Unexpected state</div>
        <div>Please try running the table diff command again.</div>
      </div>
    </div>
  )
}
