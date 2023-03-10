const sqlglotWorker = new Worker('./src/library/components/editor/worker.ts', {
  type: 'module',
})

export { sqlglotWorker }
