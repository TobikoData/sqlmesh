import { ModelLineage } from './ModelLineage'
import type {
  BrandedLineageAdjacencyList,
  BrandedLineageDetails,
  ModelLineageNodeDetails,
  ModelName,
} from './ModelLineageContext'

export default {
  title: 'Components/Lineage',
}

const adjacencyList = {
  'sqlmesh.sushi.raw_orders': ['sqlmesh.sushi.orders'],
  'sqlmesh.sushi.orders': [],
} as Record<ModelName, ModelName[]>

const lineageDetails = {
  'sqlmesh.sushi.raw_orders': {
    name: 'sqlmesh.sushi.raw_orders',
    display_name: 'sushi.raw_orders',
    identifier: '123456789',
    version: '123456789',
    dialect: 'bigquery',
    cron: '0 0 * * *',
    owner: 'admin',
    kind: 'INCREMENTAL_BY_TIME',
    model_type: 'python',
    tags: ['test', 'tag', 'another tag'],
    columns: {
      user_id: {
        data_type: 'STRING',
        description: 'node',
      },
      event_id: {
        data_type: 'STRING',
        description: 'node',
      },
      created_at: {
        data_type: 'TIMESTAMP',
        description: 'node',
      },
    },
  },
  'sqlmesh.sushi.orders': {
    name: 'sqlmesh.sushi.orders',
    display_name: 'sushi.orders',
    identifier: '123456789',
    version: '123456789',
    dialect: 'bigquery',
    cron: '0 0 * * *',
    owner: 'admin',
    kind: 'INCREMENTAL_BY_TIME',
    model_type: 'sql',
    tags: ['test', 'tag', 'another tag'],
    columns: {
      user_id: {
        data_type: 'STRING',
        description: 'node',
        columnLineageData: {
          'sqlmesh.sushi.orders': {
            user_id: {
              source: 'sqlmesh.sushi.raw_orders',
              expression: 'select user_id from sqlmesh.sushi.raw_orders',
              models: {
                'sqlmesh.sushi.raw_orders': ['user_id'],
              },
            },
          },
        },
      },
      event_id: {
        data_type: 'STRING',
        description: 'node',
        columnLineageData: {
          'sqlmesh.sushi.orders': {
            event_id: {
              models: {
                'sqlmesh.sushi.raw_orders': ['event_id'],
              },
            },
          },
        },
      },
      product_id: {
        data_type: 'STRING',
        description: 'node',
      },
      customer_id: {
        data_type: 'STRING',
        description: 'node',
      },
      updated_at: {
        data_type: 'TIMESTAMP',
        description: 'node',
      },
      deleted_at: {
        data_type: 'TIMESTAMP',
        description: 'node',
      },
      expired_at: {
        data_type: 'TIMESTAMP',
        description: 'node',
      },
      start_at: {
        data_type: 'TIMESTAMP',
        description: 'node',
      },
      end_at: {
        data_type: 'TIMESTAMP',
        description: 'node',
      },
      created_ts: {
        data_type: 'TIMESTAMP',
        description: 'node',
      },
    },
  },
} as Record<ModelName, ModelLineageNodeDetails>

export const LineageModel = () => {
  return (
    <div
      style={{
        width: '90vw',
        height: '90vh',
      }}
    >
      <style>{`
        :where(:root) {
          --color-metadata-label: rgba(100, 100, 100, 1);
          --color-metadata-value: rgba(10, 10, 10, 1);

          --color-tooltip-background: rgba(150, 150, 150, 1);
          --color-tooltip-foreground: rgba(255, 255, 255, 1);

          --color-filterable-list-counter-background: rgba(200, 0, 0, 1);
          --color-filterable-list-counter-foreground: rgba(255, 255, 255, 1);
          --color-filterable-list-input-background: rgba(250, 250, 250, 1);
          --color-filterable-list-input-foreground: rgba(0, 0, 0, 1);
          --color-filterable-list-input-placeholder: rgba(100, 100, 100, 1);
          --color-filterable-list-input-border: rgba(100, 100, 100, 1);

          --color-lineage-control-background: rgba(250, 250, 250, 1);
          --color-lineage-control-background-hover: rgba(245, 245, 245, 1);
          --color-lineage-control-icon-background: rgba(0, 0, 0, 1);
          --color-lineage-control-icon-foreground: rgba(255, 255, 255, 1);
          --color-lineage-control-button-tooltip-background: rgba(150, 150, 150, 1);
          --color-lineage-control-button-tooltip-foreground: rgba(255, 255, 255, 1);

          --color-model-name-grayscale-link-underline: rgba(125, 125, 125, 1);
          --color-model-name-grayscale-link-underline-hover: rgba(125, 125, 125, 1);
          --color-model-name-link-underline: rgba(0, 0, 0, 1);
          --color-model-name-link-underline-hover: rgba(0, 0, 0, 1);
          --color-model-name-catalog: rgba(0, 0, 0, 1);
          --color-model-name-schema: rgba(0, 0, 0, 1);
          --color-model-name-model: rgba(0, 0, 0, 1);
          --color-model-name-grayscale-catalog: rgba(100, 100, 100, 1);
          --color-model-name-grayscale-schema: rgba(50, 50, 50, 1);
          --color-model-name-grayscale-model: rgba(10, 10, 10, 1);
          --color-model-name-copy-icon: rgba(100, 100, 100, 1);
          --color-model-name-copy-icon-hover: rgba(125, 125, 125, 1);

          --color-lineage-background: rgba(255, 255, 255, 1);
          --color-lineage-divider: rgba(0, 0, 0, 0.1);
          --color-lineage-border: rgba(0, 0, 0, 0.1);

          --color-lineage-grid-dot: rgba(0, 0, 0, 0.1);

          --color-lineage-node-background: rgba(255, 255, 255, 1);
          --color-lineage-node-foreground: rgba(0, 0, 0, 0.75);
          --color-lineage-node-selected-border: rgba(0, 120, 120, 0.5);
          --color-lineage-node-border: rgba(0, 0, 0, 0.1);
          --color-lineage-node-border-hover: rgba(0, 0, 0, 0.2);

          --color-lineage-node-badge-background: rgba(200, 200, 200, 1);
          --color-lineage-node-badge-foreground: rgba(0, 0, 0, 1);

          --color-lineage-node-appendix-background: transparent;

          --color-lineage-node-type-background-sql: rgba(0, 0, 120, 1);
          --color-lineage-node-type-foreground-sql: rgba(0, 0, 120, 1);
          --color-lineage-node-type-border-sql: rgba(0, 0, 120, 1);

          --color-lineage-node-type-background-python: rgba(120, 0, 120, 1);
          --color-lineage-node-type-foreground-python: rgba(120, 0, 120, 1);
          --color-lineage-node-type-border-python: rgba(120, 0, 120, 1);

          --color-lineage-node-type-background-source: rgba(120, 120, 0, 1);
          --color-lineage-node-type-foreground-source: rgba(120, 120, 0, 1);
          --color-lineage-node-type-border-source: rgba(120, 120, 0, 1);

          --color-lineage-node-type-background-cte-subquery: rgba(120, 120, 120, 1);
          --color-lineage-node-type-foreground-cte-subquery: rgba(120, 120, 120, 1);
          --color-lineage-node-type-border-cte-subquery: rgba(120, 120, 120, 1);

          --color-lineage-node-type-handle-icon-background: rgba(255, 255, 255, 1);
          --color-lineage-node-type-handle-icon-foreground: rgba(0, 0, 0, 1);

          --color-lineage-edge: rgba(0, 0, 0, 0.1);

          --color-lineage-node-port-background: rgba(70, 0, 0, 0.05);
          --color-lineage-node-port-handle-source: rgba(70, 0, 0, 1);
          --color-lineage-node-port-handle-target: rgba(170, 0, 0, 1);
          --color-lineage-node-port-edge-source: rgba(70, 0, 0, 1);
          --color-lineage-node-port-edge-target: rgba(130, 0, 0, 1);

          --color-lineage-model-column-error-background: rgba(255, 0, 0, 1);
          --color-lineage-model-column-source-background: rgba(0, 200, 200, 1);
          --color-lineage-model-column-expression-background: rgba(0, 10, 100, 1);
          --color-lineage-model-column-error-icon: rgba(255, 0, 0, 1);
          --color-lineage-model-column-active: rgba(70, 0, 0, 0.1);
          --color-lineage-model-column-icon: rgba(0, 0, 0, 1);
          --color-lineage-model-column-icon-active: rgba(0, 0, 0, 1);
        }
      `}</style>
      <ModelLineage
        selectedModelName={'sqlmesh.sushi.orders' as ModelName}
        adjacencyList={adjacencyList as BrandedLineageAdjacencyList}
        lineageDetails={lineageDetails as BrandedLineageDetails}
        className="rounded-2xl"
      />
    </div>
  )
}
