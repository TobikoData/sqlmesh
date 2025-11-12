import { StrictMode } from 'react'
import ReactDOM from 'react-dom/client'
import reportWebVitals from './reportWebVitals.ts'
import { EventBusProvider } from './hooks/eventBus.tsx'
import { TableDiffPage } from './pages/tablediff.tsx'
import { LineagePage } from './pages/lineage.tsx'

// Detect panel type
declare global {
  interface Window {
    __SQLMESH_PANEL_TYPE__?: string
  }
}

const panelType = window.__SQLMESH_PANEL_TYPE__ || 'lineage'

// component selector
function App() {
  if (panelType === 'tablediff') {
    return <TableDiffPage />
  }

  return <LineagePage />
}

// Render the app
const rootElement = document.getElementById('app')
if (rootElement && !rootElement.innerHTML) {
  const root = ReactDOM.createRoot(rootElement)

  root.render(
    <StrictMode>
      <EventBusProvider>
        <App />
      </EventBusProvider>
    </StrictMode>,
  )
}

// If you want to start measuring performance in your app, pass a function
// to log results (for example: reportWebVitals(console.log))
// or send to an analytics endpoint. Learn more: https://bit.ly/CRA-vitals
reportWebVitals()
