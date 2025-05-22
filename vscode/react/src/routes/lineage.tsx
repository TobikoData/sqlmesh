import { createFileRoute } from '@tanstack/react-router'
import { LineagePage } from '@/pages/lineage'

export const Route = createFileRoute('/lineage')({
  component: LineagePage,
})
