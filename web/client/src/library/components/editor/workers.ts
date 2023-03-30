const sqlglotWorker = new Worker(new URL('./worker.ts', import.meta.url), {
  type: 'module',
})

export { sqlglotWorker }
