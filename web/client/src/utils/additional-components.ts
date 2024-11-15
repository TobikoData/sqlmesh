import React from 'react'
import { createComponent } from '@lit/react'

import {
  ResizeObserver,
  ModelName,
  SourceListSection,
  SourceListItem,
  SourceList,
  Badge,
} from '~/utils/tbk-components'

export const TBKBadge = createComponent({
  tagName: 'tbk-badge',
  elementClass: Badge,
  react: React,
})
export const TBKSourceList = createComponent({
  tagName: 'tbk-source-list',
  elementClass: SourceList,
  react: React,
  events: {
    onChange: 'change',
  },
})
export const TBKSourceListItem = createComponent({
  tagName: 'tbk-source-list-item',
  elementClass: SourceListItem,
  react: React,
})
export const TBKSourceListSection = createComponent({
  tagName: 'tbk-source-list-section',
  elementClass: SourceListSection,
  react: React,
})
export const TBKModelName = createComponent({
  tagName: 'tbk-model-name',
  elementClass: ModelName,
  react: React,
})
export const TBKResizeObserver = createComponent({
  tagName: 'tbk-resize-observer',
  elementClass: ResizeObserver,
  react: React,
})
