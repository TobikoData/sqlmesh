import { Outlet, createRootRoute } from '@tanstack/react-router'
import { TanStackRouterDevtools } from '@tanstack/react-router-devtools'
import '../App.css'
import { LineagePage } from '@/pages/lineage'
import { TableDiffPage } from '@/pages/tablediff'

export const Route = createRootRoute({
  component: () => {
    return (
      <>
        <Outlet />
        <TanStackRouterDevtools />
      </>
    )
  },
  notFoundComponent: () => {
    // switch to lineage or table diff based on panel type
    const panelType = (window as any).__SQLMESH_PANEL_TYPE__ || 'lineage'
    return panelType === 'tablediff' ? <TableDiffPage /> : <LineagePage />
  },
})
