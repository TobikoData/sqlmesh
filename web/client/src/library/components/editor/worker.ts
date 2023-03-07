import 'https://cdn.jsdelivr.net/pyodide/v0.22.1/full/pyodide.js'

const global = self as any

export {}

async function loadPyodideAndPackages(): Promise<any[]> {
  global.pyodide = await global.loadPyodide()
  await global.pyodide.loadPackage('micropip')

  const micropip = global.pyodide.pyimport('micropip')
  await micropip.install('sqlglot')
  const file = await (await fetch('./sqlglot.py')).text()

  return Array.from(global.pyodide.runPython(file))
}

const pyodideReadyPromise = loadPyodideAndPackages()

global.onmessage = async (e: MessageEvent) => {
  const [transpile, parse] = await pyodideReadyPromise

  if (e.data.topic === 'transpile') {
    global.postMessage({
      topic: 'transpile',
      payload: toArray(transpile(e.data.payload, 'duckdb')),
    })
  }

  if (e.data.topic === 'parse') {
    global.postMessage({
      topic: 'parse',
      payload: JSON.parse(parse(e.data.payload)),
    })
  }
}

global.postMessage({ topic: 'init' })

function toArray<T>(proxy: ArrayLike<T>): T[] {
  return Array.from(proxy)
}
