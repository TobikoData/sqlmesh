export default {
  theme: {
    colors: {},
    extend: {
      colors: {
        lineage: {
          background: 'var(--color-lineage-background)',
          divider: 'var(--color-lineage-divider)',
          border: 'var(--color-lineage-border)',
          control: {
            background: {
              DEFAULT: 'var(--color-lineage-control-background)',
              hover: 'var(--color-lineage-control-background-hover)',
            },
            icon: {
              background: 'var(--color-lineage-control-icon-background)',
              foreground: 'var(--color-lineage-control-icon-foreground)',
            },
          },
          grid: {
            dot: 'var(--color-lineage-grid-dot)',
          },
          node: {
            background: 'var(--color-lineage-node-background)',
            foreground: 'var(--color-lineage-node-foreground)',
            selected: {
              border: 'var(--color-lineage-node-selected-border)',
            },
            border: {
              DEFAULT: 'var(--color-lineage-node-border)',
              hover: 'var(--color-lineage-node-border-hover)',
            },
            badge: {
              background: 'var(--color-lineage-node-badge-background)',
              foreground: 'var(--color-lineage-node-badge-foreground)',
            },
            appendix: {
              background: 'var(--color-lineage-node-appendix-background)',
            },
            type: {
              background: {
                sql: 'var(--color-lineage-node-type-background-sql)',
                python: 'var(--color-lineage-node-type-background-python)',
              },
              foreground: {
                sql: 'var(--color-lineage-node-type-foreground-sql)',
                python: 'var(--color-lineage-node-type-foreground-python)',
              },
              border: {
                sql: 'var(--color-lineage-node-type-border-sql)',
                python: 'var(--color-lineage-node-type-border-python)',
              },
            },
            handle: {
              icon: {
                background:
                  'var(--color-lineage-node-type-handle-icon-background)',
              },
            },
            port: {
              background: 'var(--color-lineage-node-port-background)',
              handle: {
                target: 'var(--color-lineage-node-port-handle-target)',
                source: 'var(--color-lineage-node-port-handle-source)',
              },
              edge: {
                source: 'var(--color-lineage-node-port-edge-source)',
                target: 'var(--color-lineage-node-port-edge-target)',
              },
            },
          },
          model: {
            column: {
              source: {
                background:
                  'var(--color-lineage-model-column-source-background)',
              },
              expression: {
                background:
                  'var(--color-lineage-model-column-expression-background)',
              },
              error: {
                background:
                  'var(--color-lineage-model-column-error-background)',
                icon: 'var(--color-lineage-model-column-error-icon)',
              },
              active: 'var(--color-lineage-model-column-active)',
              icon: {
                DEFAULT: 'var(--color-lineage-model-column-icon)',
                active: 'var(--color-lineage-model-column-icon-active)',
              },
            },
          },
        },
      },
    },
  },
}
