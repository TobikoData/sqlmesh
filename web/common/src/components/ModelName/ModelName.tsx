import { Box, Check, Copy } from 'lucide-react'
import { useMemo } from 'react'

import { cn, truncate } from '@sqlmesh-common/utils'
import { Tooltip } from '@sqlmesh-common/components/Tooltip/Tooltip'
import React from 'react'

import './ModelName.css'
import { CopyButton } from '../CopyButton/CopyButton'

export interface ModelNameProps extends React.HTMLAttributes<HTMLDivElement> {
  name: string
  hideCatalog?: boolean
  hideSchema?: boolean
  hideIcon?: boolean
  showTooltip?: boolean
  showCopy?: boolean
  truncateMaxChars?: number
  truncateLimitBefore?: number
  truncateLimitAfter?: number
  grayscale?: boolean
  renderLink?: (modelName: React.ReactNode) => React.ReactNode
  className?: string
}

const MODEL_NAME_TOOLTIP_SIDE_OFFSET = 6
const MODEL_NAME_ICON_SIZE = 16

export const ModelName = React.forwardRef<HTMLDivElement, ModelNameProps>(
  (
    {
      name,
      hideCatalog = false,
      hideSchema = false,
      hideIcon = false,
      showTooltip = true,
      showCopy = false,
      truncateMaxChars = 25,
      truncateLimitBefore = 5,
      truncateLimitAfter = 7,
      grayscale = false,
      renderLink,
      className,
      ...props
    },
    ref,
  ) => {
    if (!name) throw new Error('Model name should not be empty')

    const truncateMaxCharsModel = truncateMaxChars * 2

    const { catalog, schema, model, withTooltip } = useMemo(() => {
      const [model, schema, catalog] = name.split('.').reverse()

      return {
        catalog: hideCatalog ? undefined : catalog,
        schema: hideSchema ? undefined : schema,
        model,
        withTooltip:
          ((hideCatalog && catalog) ||
            (hideSchema && schema) ||
            [catalog, schema].some(v => v && v.length > truncateMaxChars) ||
            model.length > truncateMaxCharsModel) &&
          showTooltip,
      }
    }, [
      name,
      hideCatalog,
      hideSchema,
      truncateMaxCharsModel,
      showTooltip,
      truncateMaxChars,
    ])

    function renderTooltip() {
      return (
        <Tooltip
          trigger={renderName()}
          sideOffset={MODEL_NAME_TOOLTIP_SIDE_OFFSET}
          side="top"
          className="text-xs px-2 py-1 rounded-sm font-semibold"
        >
          {name}
        </Tooltip>
      )
    }

    function renderIcon() {
      return (
        <Box
          size={MODEL_NAME_ICON_SIZE}
          className={cn(
            'mr-1 flex-shrink-0',
            grayscale
              ? 'text-model-name-grayscale-model'
              : 'text-model-name-model',
            renderLink && '-mt-[4px]',
          )}
        />
      )
    }

    console.assert(name.length > 0, 'Model name should not be empty')

    function renderName() {
      return (
        <span
          data-testid="model-name"
          className="flex overflow-hidden"
        >
          {catalog && (
            <>
              <span
                className={cn(
                  grayscale
                    ? 'text-model-name-grayscale-catalog'
                    : 'text-model-name-catalog',
                )}
              >
                {_truncate(catalog)}
              </span>
              .
            </>
          )}
          {schema && (
            <>
              <span
                className={cn(
                  grayscale
                    ? 'text-model-name-grayscale-schema'
                    : 'text-model-name-schema',
                )}
              >
                {_truncate(schema)}
              </span>
              .
            </>
          )}
          <span
            className={cn(
              'truncate',
              grayscale
                ? 'text-model-name-grayscale-model'
                : 'text-model-name-model',
            )}
          >
            {truncate(
              model,
              truncateMaxCharsModel,
              truncateLimitBefore * 2,
              '...',
              truncateLimitBefore * 2,
            )}
          </span>
        </span>
      )
    }

    function renderNameWithTooltip() {
      return withTooltip ? renderTooltip() : renderName()
    }

    function _truncate(name: string, maxChars: number = truncateMaxChars) {
      return truncate(
        name,
        maxChars,
        truncateLimitBefore,
        '...',
        truncateLimitAfter,
      )
    }

    return (
      <span
        ref={ref}
        data-component="ModelName"
        className={cn(
          'inline-flex items-center whitespace-nowrap overflow-hidden font-semibold',
          className,
        )}
        {...props}
      >
        {!hideIcon && renderIcon()}
        {renderLink ? (
          <span
            className={cn(
              'flex cursor-pointer border-b -mt-0.5 text-inherit',
              grayscale
                ? 'border-model-name-grayscale-link-underline hover:border-model-name-grayscale-link-underline-hover'
                : 'border-model-name-link-underline hover:border-model-name-link-underline-hover',
            )}
          >
            {renderLink(renderNameWithTooltip())}
          </span>
        ) : (
          renderNameWithTooltip()
        )}
        {showCopy && (
          <CopyButton
            size="2xs"
            variant="transparent"
            text={name}
            className="ml-2 w-6 hover:text-model-name-copy-icon-hover active:text-model-name-copy-icon-hover bg-model-name-copy-icon-background hover:bg-model-name-copy-icon-background-hover active:bg-model-name-copy-icon-background-hover"
          >
            {copied =>
              copied ? (
                <Check
                  size={MODEL_NAME_ICON_SIZE}
                  className="text-model-name-copy-icon"
                />
              ) : (
                <Copy
                  size={MODEL_NAME_ICON_SIZE}
                  className="text-model-name-copy-icon"
                />
              )
            }
          </CopyButton>
        )}
      </span>
    )
  },
)

ModelName.displayName = 'ModelName'
