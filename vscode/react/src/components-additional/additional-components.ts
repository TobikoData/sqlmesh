// @ts-nocheck
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
  TextBlock,
  Information,
  Icon,
  Button,
  Tooltip,
} from '@/utils/tbk-components'

Button.defineAs('tbk-button')
Icon.defineAs('tbk-icon')
Tooltip.defineAs('tbk-tooltip')
Information.defineAs('tbk-information')
TextBlock.defineAs('tbk-text-block')
Datetime.defineAs('tbk-datetime')
Details.defineAs('tbk-details')
Tab.defineAs('tbk-tab')
TabPanel.defineAs('tbk-tab-panel')
Tabs.defineAs('tbk-tabs')
SplitPane.defineAs('tbk-split-pane')
Scroll.defineAs('tbk-scroll')
MetadataSection.defineAs('tbk-metadata-section')
MetadataItem.defineAs('tbk-metadata-item')
Metadata.defineAs('tbk-metadata')
Badge.defineAs('tbk-badge')
ModelName.defineAs('tbk-model-name')
ResizeObserver.defineAs('tbk-resize-observer')
SourceList.defineAs('tbk-ui-source-list')
SourceListItem.defineAs('tbk-ui-source-list-item')
SourceListSection.defineAs('tbk-ui-source-list-section')

export const TBKButton = createComponent({
  tagName: 'tbk-button',
  elementClass: Button,
  react: React,
}) as any
export const TBKIcon = createComponent({
  tagName: 'tbk-icon',
  elementClass: Icon,
  react: React,
}) as any
export const TBKTooltip = createComponent({
  tagName: 'tbk-tooltip',
  elementClass: Tooltip,
  react: React,
}) as any
export const TBKInformation = createComponent({
  tagName: 'tbk-information',
  elementClass: Information,
  react: React,
}) as any
export const TBKTextBlock = createComponent({
  tagName: 'tbk-text-block',
  elementClass: TextBlock,
  react: React,
}) as any
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

export const TBKSourceList = createComponent({
  tagName: 'tbk-ui-source-list',
  elementClass: SourceList,
  react: React,
  events: {
    onChange: 'change',
  },
}) as any
export const TBKSourceListItem = createComponent({
  tagName: 'tbk-ui-source-list-item',
  elementClass: SourceListItem,
  react: React,
}) as any
export const TBKSourceListSection = createComponent({
  tagName: 'tbk-ui-source-list-section',
  elementClass: SourceListSection,
  react: React,
}) as any
