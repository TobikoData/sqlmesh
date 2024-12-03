import createLineageWorker from './lineage.ts?worker&inline'
import createSqlglotWorker from './sqlglot/sqlglot.ts?worker&inline'

const sqlglotWorker = createSqlglotWorker()

export { sqlglotWorker, createLineageWorker }
