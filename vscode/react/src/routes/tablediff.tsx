import { createFileRoute } from '@tanstack/react-router'
import { TableDiffPage } from '../pages/tablediff'

export const Route = createFileRoute('/tablediff')({
  component: TableDiffPage,
})
