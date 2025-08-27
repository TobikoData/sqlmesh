import { Box, Check, Copy } from 'lucide-react'
import { useMemo } from 'react'

import { ClipboardCopy } from '@/components/ClipboardCopy/ClipboardCopy'
import { cn } from '@/utils'
import { EnumSize } from '@/types/enums'
import { isNilOrEmptyString, truncate } from '@/utils'
import Tooltip from '@/components/Tooltip/Tooltip'

const MODEL_NAME_TOOLTIP_SIDE_OFFSET = 6
const MODEL_NAME_ICON_SIZE = 16

export function ModelName({
  name,
  hideCatalog = false,
  hideSchema = false,
  hideIcon = false,
  showTooltip = false,
  showCopy = false,
  truncateMaxChars = 25,
  truncateLimitBefore = 5,
  truncateLimitAfter = 7,
  grayscale = false,
  link,
  className,
}: {
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
  link?: string
  className?: string
}) {
  if (isNilOrEmptyString(name))
    throw new Error('Model name should not be empty')

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
          link && '-mt-[4px]',
        )}
      />
    )
  }

  console.assert(model, 'Model name should not be empty')

  function renderName() {
    return (
      <span
        data-testid="model-name"
        className="overflow-hidden"
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
            grayscale
              ? 'text-model-name-grayscale-model'
              : 'text-model-name-model',
          )}
        >
          {truncate(model, truncateMaxCharsModel, 15)}
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
      data-component="ModelName"
      className={cn(
        'inline-flex items-center whitespace-nowrap overflow-hidden font-semibold',
        className,
      )}
    >
      {!hideIcon && renderIcon()}
      {link ? (
        <a
          href={link}
          className={cn(
            'flex cursor-pointer border-b -mt-0.5 text-inherit',
            grayscale
              ? 'border-model-name-grayscale-link hover:border-model-name-grayscale-link-hover'
              : 'border-model-name-link hover:border-model-name-link-hover',
          )}
        >
          {renderNameWithTooltip()}
        </a>
      ) : (
        renderNameWithTooltip()
      )}
      {showCopy && (
        <ClipboardCopy
          size={EnumSize.XXS}
          text={name}
          className="ml-2 w-6 hover:text-model-name-copy-icon-hover active:text-model-name-copy-icon-hover"
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
        </ClipboardCopy>
      )}
    </span>
  )
}
