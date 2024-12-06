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
}) as any
export const TBKDetails = createComponent({
  tagName: 'tbk-details',
  elementClass: Details,
  react: React,
}) as any
export const TBKTabs = createComponent({
  tagName: 'tbk-tabs',
  elementClass: Tabs,
  react: React,
}) as any
export const TBKTab = createComponent({
  tagName: 'tbk-tab',
  elementClass: Tab,
  react: React,
}) as any
export const TBKTabPanel = createComponent({
  tagName: 'tbk-tab-panel',
  elementClass: TabPanel,
  react: React,
}) as any
export const TBKSplitPane = createComponent({
  tagName: 'tbk-split-pane',
  elementClass: SplitPane,
  react: React,
  events: {
    onChange: 'sl-reposition',
  },
}) as any
export const TBKScroll = createComponent({
  tagName: 'tbk-scroll',
  elementClass: Scroll,
  react: React,
}) as any
export const TBKMetadataSection = createComponent({
  tagName: 'tbk-metadata-section',
  elementClass: MetadataSection,
  react: React,
}) as any
export const TBKMetadataItem = createComponent({
  tagName: 'tbk-metadata-item',
  elementClass: MetadataItem,
  react: React,
}) as any
export const TBKMetadata = createComponent({
  tagName: 'tbk-metadata',
  elementClass: Metadata,
  react: React,
}) as any
export const TBKBadge = createComponent({
  tagName: 'tbk-badge',
  elementClass: Badge,
  react: React,
}) as any
export const TBKSourceList = createComponent({
  tagName: 'tbk-source-list',
  elementClass: SourceList,
  react: React,
  events: {
    onChange: 'change',
  },
}) as any
export const TBKSourceListItem = createComponent({
  tagName: 'tbk-source-list-item',
  elementClass: SourceListItem,
  react: React,
}) as any
export const TBKSourceListSection = createComponent({
  tagName: 'tbk-source-list-section',
  elementClass: SourceListSection,
  react: React,
}) as any
export const TBKModelName = createComponent({
  tagName: 'tbk-model-name',
  elementClass: ModelName,
  react: React,
}) as any
export const TBKResizeObserver = createComponent({
  tagName: 'tbk-resize-observer',
  elementClass: ResizeObserver,
  react: React,
}) as any
