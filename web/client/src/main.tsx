import React, { type HTMLAttributes } from 'react'
import ThemeProvider from '@context/theme'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import App from './App'

import './index.css'

export interface PropsComponent extends HTMLAttributes<HTMLElement> {}

const client = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: false,
      staleTime: Infinity,
    },
  },
})

ReactDOM.createRoot(getRootNode()).render(
  <React.StrictMode>
    <QueryClientProvider client={client}>
      <ThemeProvider>
        <App />
      </ThemeProvider>
    </QueryClientProvider>
  </React.StrictMode>,
)

function getRootNode(): HTMLElement {
  const id = 'root'

  let elRoot = document.getElementById(id)

  if (elRoot != null) return elRoot

  const elBody = document.body
  const firstChild = elBody.children[0]

  elRoot = document.createElement('div')
  elRoot.id = id
  elRoot.className = 'h-full w-full flex flex-col justify-start'

  if (firstChild instanceof HTMLElement) {
    elBody.insertBefore(elRoot, firstChild)
  } else {
    elBody.appendChild(elRoot)
  }

  return elRoot
}
