declare function importScripts(...urls: string[]): void

importScripts('https://cdn.jsdelivr.net/pyodide/v0.22.1/full/pyodide.js')

const scope = self as any

async function loadPyodideAndPackages(): Promise<any[]> {
  scope.pyodide = await scope.loadPyodide()

  await scope.pyodide.loadPackage('micropip')

  const micropip = scope.pyodide.pyimport('micropip')

  await micropip.install('sqlglot')

  const file = await (
    await fetch(new URL('./sqlglot.py', import.meta.url))
  ).text()

  scope.postMessage({ topic: 'init' })

  return Array.from(scope.pyodide.runPython(file))
}

const pyodideReadyPromise = loadPyodideAndPackages()

scope.onmessage = async (e: MessageEvent) => {
  const [parse, get_dialect, dialects] = await pyodideReadyPromise

  if (e.data.topic === 'parse') {
    let payload

    try {
      payload = JSON.parse(parse(e.data.payload))
    } catch (error) {
      payload = {
        type: 'error',
        message: 'Invalid JSON',
      }
    }

    scope.postMessage({
      topic: 'parse',
      payload,
    })
  }

  if (e.data.topic === 'dialect') {
    const { keywords, types }: { keywords: string; types: string } = JSON.parse(
      get_dialect(e.data.payload),
    )

    scope.postMessage({
      topic: 'dialect',
      payload: {
        types: `${types} `.toLowerCase(),
        keywords: `${keywords} `.toLowerCase(),
      },
    })
  }

  if (e.data.topic === 'dialects') {
    scope.postMessage({
      topic: 'dialects',
      payload: {
        dialects: JSON.parse(dialects),
        dialect: 'mysql',
      },
    })
  }
}
