import React from 'react'
import { createComponent } from '@lit/react'

import {
  ResizeObserver,
  ModelName,
  SourceListSection,
  SourceListItem,
  SourceList,
  Badge,
  Metadata,
  MetadataItem,
  MetadataSection,
  Scroll,
  SplitPane,
  Details,
  Datetime,
  Tab,
  Tabs,
  TabPanel,
} from '~/utils/tbk-components'

export const TBKDatetime = createComponent({
  tagName: 'tbk-datetime',
  elementClass: Datetime,
  react: React,
})
export const TBKDetails = createComponent({
  tagName: 'tbk-details',
  elementClass: Details,
  react: React,
})
export const TBKTabs = createComponent({
  tagName: 'tbk-tabs',
  elementClass: Tabs,
  react: React,
})
export const TBKTab = createComponent({
  tagName: 'tbk-tab',
  elementClass: Tab,
  react: React,
})
export const TBKTabPanel = createComponent({
  tagName: 'tbk-tab-panel',
  elementClass: TabPanel,
  react: React,
})
export const TBKSplitPane = createComponent({
  tagName: 'tbk-split-pane',
  elementClass: SplitPane,
  react: React,
  events: {
    onChange: 'sl-reposition',
  },
})
export const TBKScroll = createComponent({
  tagName: 'tbk-scroll',
  elementClass: Scroll,
  react: React,
})
export const TBKMetadataSection = createComponent({
  tagName: 'tbk-metadata-section',
  elementClass: MetadataSection,
  react: React,
})
export const TBKMetadataItem = createComponent({
  tagName: 'tbk-metadata-item',
  elementClass: MetadataItem,
  react: React,
})
export const TBKMetadata = createComponent({
  tagName: 'tbk-metadata',
  elementClass: Metadata,
  react: React,
})
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
