const sqlglotWorker = new Worker(
  new URL('./sqlglot/sqlglot.ts', import.meta.url),
)

function createLineageWorker(): Worker {
  return new Worker(new URL('./lineage.ts', import.meta.url), {
    type: 'module',
  })
}

export { sqlglotWorker, createLineageWorker }
