declare function importScripts(...urls: string[]): void

importScripts('https://cdn.jsdelivr.net/pyodide/v0.23.2/full/pyodide.js')

const scope = self as any

async function loadPyodideAndPackages(): Promise<{
  sqlglot: Record<string, any>
}> {
  scope.pyodide = await scope.loadPyodide()

  await scope.pyodide.loadPackage('micropip')

  const micropip = scope.pyodide.pyimport('micropip')

  await micropip.install('sqlglot')
  await micropip.install('typing-extensions')

  const file = await (
    await fetch(new URL('./sqlglot.py', import.meta.url))
  ).text()

  scope.postMessage({
    topic: 'init',
  })

  return {
    sqlglot: scope.pyodide.runPython(file),
  }
}

const pyodideReadyPromise = loadPyodideAndPackages()

scope.onmessage = async (e: MessageEvent) => {
  const { sqlglot } = await pyodideReadyPromise

  if (e.data.topic === 'validate') {
    let payload: boolean

    try {
      payload = JSON.parse(sqlglot.get('validate')?.(e.data.payload))
    } catch (error) {
      payload = false
    }

    scope.postMessage({
      topic: 'validate',
      payload,
    })
  }

  if (e.data.topic === 'dialect') {
    let payload: { keywords: string; types: string }

    try {
      payload = JSON.parse(sqlglot.get('get_dialect')?.(e.data.payload))
    } catch (error) {
      payload = {
        keywords: '',
        types: '',
      }
    }

    scope.postMessage({
      topic: 'dialect',
      payload,
    })
  }

  if (e.data.topic === 'dialects') {
    let payload: Array<{ dialect_title: string; dialect_name: string }>

    try {
      payload = JSON.parse(sqlglot.get('dialects'))
    } catch (error) {
      payload = []
    }

    scope.postMessage({
      topic: 'dialects',
      payload,
    })
  }

  if (e.data.topic === 'format') {
    scope.postMessage({
      topic: 'format',
      payload: sqlglot.get('format')?.(e.data.payload.sql) ?? '',
    })
  }

  if (e.data.topic === 'init') {
    scope.postMessage({
      topic: 'init',
    })
  }
}
