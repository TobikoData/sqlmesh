import 'https://cdn.jsdelivr.net/pyodide/v0.22.1/full/pyodide.js'

export {}

const global = self as any

async function loadPyodideAndPackages(): Promise<any[]> {
  global.pyodide = await global.loadPyodide()
  await global.pyodide.loadPackage('micropip')

  const micropip = global.pyodide.pyimport('micropip')
  await micropip.install('sqlglot')
  const file = await (await fetch('./sqlglot.py')).text()

  global.postMessage({ topic: 'init' })

  return Array.from(global.pyodide.runPython(file))
}

const pyodideReadyPromise = loadPyodideAndPackages()

global.onmessage = async (e: MessageEvent) => {
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

    global.postMessage({
      topic: 'parse',
      payload,
    })
  }

  if (e.data.topic === 'dialect') {
    const { keywords, types }: { keywords: string; types: string } = JSON.parse(
      get_dialect(e.data.payload),
    )

    global.postMessage({
      topic: 'dialect',
      payload: {
        types: `${types} `.toLowerCase(),
        keywords: `${keywords} `.toLowerCase(),
      },
    })
  }

  if (e.data.topic === 'dialects') {
    global.postMessage({
      topic: 'dialects',
      payload: {
        dialects: JSON.parse(dialects),
        dialect: 'mysql',
      },
    })
  }
}
