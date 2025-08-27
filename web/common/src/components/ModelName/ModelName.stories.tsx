import type { Meta, StoryObj } from '@storybook/react-vite'

import { ModelName } from './ModelName'

const meta: Meta<typeof ModelName> = {
  title: 'Components/ModelName',
  component: ModelName,
  tags: ['autodocs'],
  argTypes: {
    name: { control: 'text' },
    hideCatalog: { control: 'boolean' },
    hideSchema: { control: 'boolean' },
    hideIcon: { control: 'boolean' },
    showTooltip: { control: 'boolean' },
    className: { control: 'text' },
  },
}

export default meta
type Story = StoryObj<typeof ModelName>

export const Default: Story = {
  args: {
    name: 'catalog.schema.model',
  },
}

export const WithoutCatalog: Story = {
  args: {
    name: 'catalog.schema.model',
    hideCatalog: true,
  },
}

export const WithoutSchema: Story = {
  args: {
    name: 'catalog.schema.model',
    hideSchema: true,
  },
}

export const WithoutIcon: Story = {
  args: {
    name: 'catalog.schema.model',
    hideIcon: true,
  },
}

export const WithTooltip: Story = {
  args: {
    name: 'catalog.schema.model',
    hideCatalog: true,
    hideSchema: true,
    showTooltip: true,
  },
}

export const WithoutTooltip: Story = {
  args: {
    name: 'catalog.schema.model',
    showTooltip: false,
  },
}

export const CustomClassName: Story = {
  args: {
    name: 'catalog.schema.model',
    className: 'text-xl font-bold',
  },
}

export const LongName: Story = {
  args: {
    name: 'veryveryverylongcatalogname.veryveryverylongschamename.veryveryverylongmodelnameveryveryverylongmodelname',
  },
}

export const Grayscale: Story = {
  args: {
    name: 'catalog.schema.model',
    grayscale: true,
  },
}

export const Link: Story = {
  args: {
    name: 'catalog.schema.model',
    link: 'https://www.google.com',
    grayscale: true,
    showCopy: true,
  },
}
