import { useState, useEffect } from 'react'
import { callRpc } from '../../utils/rpc'
import type { TableDiffData, TableDiffParams } from './types'

interface RerunControllerProps {
  data: TableDiffData
  onDataUpdate?: (data: TableDiffData) => void
  children: (props: {
    limit: number
    whereClause: string
    onColumns: string
    isRerunning: boolean
    hasChanges: boolean
    setLimit: (limit: number) => void
    setWhereClause: (where: string) => void
    setOnColumns: (on: string) => void
    handleRerun: () => void
  }) => React.ReactNode
}

export function RerunController({
  data,
  onDataUpdate,
  children,
}: RerunControllerProps) {
  const [isRerunning, setIsRerunning] = useState(false)
  const [limit, setLimit] = useState(data.limit || 20)
  const [whereClause, setWhereClause] = useState(data.where || '')
  const [onColumns, setOnColumns] = useState(
    data.on?.map(([sCol, tCol]) => `s.${sCol} = t.${tCol}`).join(' AND ') || '',
  )

  // Update state when data changes
  useEffect(() => {
    setLimit(data.limit || 20)
    setWhereClause(data.where || '')
    setOnColumns(
      data.on?.map(([sCol, tCol]) => `s.${sCol} = t.${tCol}`).join(' AND ') ||
        '',
    )
  }, [data.limit, data.where, data.on])

  // Helper function to parse on columns back to array format
  const parseOnColumns = (onString: string): string[][] => {
    if (!onString.trim()) return []

    // Parse "s.id = t.id AND s.date = t.date" back to [["id", "id"], ["date", "date"]]
    const conditions = onString.split(' AND ')
    return conditions.map(condition => {
      const match = condition.trim().match(/^s\.(\w+)\s*=\s*t\.(\w+)$/)
      if (match) {
        return [match[1], match[2]]
      }
      // Fallback for simple format
      return [condition.trim(), condition.trim()]
    })
  }

  const hasChanges =
    limit !== (data.limit || 20) ||
    whereClause !== (data.where || '') ||
    onColumns !==
      (data.on?.map(([sCol, tCol]) => `s.${sCol} = t.${tCol}`).join(' AND ') ||
        '')

  const handleRerun = async () => {
    if (isRerunning || !hasChanges) return

    setIsRerunning(true)
    try {
      // Get the initial data to extract the model name and environment names
      const initialDataResult = await callRpc('get_initial_data', {})
      if (!initialDataResult.ok || !initialDataResult.value?.selectedModel) {
        console.error('Failed to get initial data for rerun')
        return
      }

      const params: TableDiffParams = {
        source: initialDataResult.value.sourceEnvironment || 'prod',
        target: initialDataResult.value.targetEnvironment || 'dev',
        model_or_snapshot: initialDataResult.value.selectedModel.name,
        limit: Math.min(Math.max(1, limit), 10000), // Ensure limit is within bounds
        ...(whereClause.trim() && { where: whereClause.trim() }),
        ...(onColumns.trim() && { on: onColumns.trim() }),
      }

      console.log('Rerunning table diff with params:', params)

      try {
        const timeoutPromise = new Promise((_, reject) => {
          setTimeout(() => reject(new Error('Request timeout')), 30000) // 30 second timeout
        })

        const apiPromise = callRpc('api_query', {
          method: 'GET',
          url: '/api/table_diff',
          params: params,
          body: {},
        })

        const result = (await Promise.race([apiPromise, timeoutPromise])) as any

        console.log('Table diff result:', result)

        if (result.ok && result.value) {
          let newData: TableDiffData
          if (result.value.data) {
            newData = {
              ...result.value.data,
              limit,
              where: whereClause,
              on: parseOnColumns(onColumns),
            }
          } else {
            newData = {
              ...result.value,
              limit,
              where: whereClause,
              on: parseOnColumns(onColumns),
            }
          }

          console.log('Updating table diff data:', newData)
          onDataUpdate?.(newData)
        } else {
          console.error('API call failed:', result.error)
          // Try to extract meaningful error message
          let errorMessage = 'Unknown error'
          if (typeof result.error === 'string') {
            try {
              const parsed = JSON.parse(result.error)
              errorMessage = parsed.message || parsed.code || result.error
            } catch {
              errorMessage = result.error
            }
          }
          console.error('Processed error message:', errorMessage)
          setIsRerunning(false)
          return
        }
      } catch (apiError) {
        console.error('API call threw exception:', apiError)
        setIsRerunning(false)
        return
      }
    } catch (error) {
      console.error('Error rerunning table diff:', error)
    } finally {
      setIsRerunning(false)
    }
  }

  return (
    <>
      {children({
        limit,
        whereClause,
        onColumns,
        isRerunning,
        hasChanges,
        setLimit,
        setWhereClause,
        setOnColumns,
        handleRerun,
      })}
    </>
  )
}
