const sqlglotWorker = new Worker(
  new URL('./sqlglot/worker.ts', import.meta.url),
)

export { sqlglotWorker }
