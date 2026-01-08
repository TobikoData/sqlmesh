import type { Meta, StoryObj } from '@storybook/react-vite'

import { VerticalContainer } from '../VerticalContainer/VerticalContainer'
import { Description } from '../Typography/Description'
import { Metadata } from '../Metadata/Metadata'
import { FilterableList } from './FilterableList'
import { VirtualList } from './VirtualList'
import { Badge } from '../Badge/Badge'

interface MockItem {
  id: string
  name: string
  description: string
  category?: string
  isActive?: boolean
}

const meta: Meta<typeof VirtualList> = {
  title: 'Components/VirtualList',
  component: VirtualList,
  decorators: [
    Story => (
      <div
        style={{
          height: '400px',
          border: '1px solid #e5e5e5',
          borderRadius: '8px',
        }}
      >
        <Story />
      </div>
    ),
  ],
  argTypes: {
    estimatedListItemHeight: {
      control: 'number',
      description: 'Estimated height of each item in pixels',
    },
    className: {
      control: 'text',
      description: 'Additional CSS classes',
    },
  },
}

export default meta
type Story = StoryObj<typeof VirtualList<MockItem>>

const generateMockItems = (count: number): MockItem[] => {
  const categories = ['Documents', 'Images', 'Videos', 'Audio', 'Other']
  return Array.from({ length: count }, (_, i) => ({
    id: `item-${i}`,
    name: `Item ${i + 1}`,
    description: `This is the description for item ${i + 1}`,
    category: categories[i % categories.length],
    isActive: i === 10,
  }))
}

function renderListItem(item: MockItem, height = 48) {
  return (
    <VerticalContainer
      className="h-12 py-1"
      style={{ height: `${height}px` }}
    >
      <Metadata
        key={item.id}
        label={item.name}
        value={<Badge>{item.category}</Badge>}
        className="h-6"
      />
      <Description className="text-xs">{item.description}</Description>
    </VerticalContainer>
  )
}

export const Default: Story = {
  args: {
    items: generateMockItems(100),
    estimatedListItemHeight: 48,
    renderListItem,
  },
}

export const WithSelection: Story = {
  args: {
    items: generateMockItems(100),
    isSelected: (item: MockItem) => item.isActive === true,
    estimatedListItemHeight: 48,
    renderListItem,
  },
}

export const LargeDataset: Story = {
  args: {
    items: generateMockItems(10000),
    estimatedListItemHeight: 48,
    renderListItem,
  },
}

export const VariableHeightContent: Story = {
  args: {
    items: generateMockItems(50).map((item, i) => ({
      ...item,
      description:
        i % 3 === 0
          ? `This is a much longer description for ${item.name} that will wrap to multiple lines and demonstrate how the virtual list handles variable content heights.`
          : item.description,
    })),
    estimatedListItemHeight: 80,
    renderListItem: item => renderListItem(item, 80),
  },
}

export const EmptyState: Story = {
  args: {
    items: [],
    estimatedListItemHeight: 60,
    renderListItem: () => null,
  },
}

export const SingleItem: Story = {
  args: {
    items: generateMockItems(1),
    estimatedListItemHeight: 48,
    renderListItem,
  },
}

export const WithScrollToSelected: Story = {
  args: {
    items: generateMockItems(200).map((item, i) => ({
      ...item,
      isActive: i === 150,
    })),
    estimatedListItemHeight: 48,
    isSelected: (item: MockItem) => item.isActive === true,
    renderListItem,
  },
}

// FilterableList Stories
export const FilterableListDefault: Story = {
  render: () => {
    const items = generateMockItems(50)
    return (
      <FilterableList
        items={items}
        placeholder="Search items..."
        filterOptions={{
          keys: ['name', 'description'],
          threshold: 0.3,
        }}
      >
        {filteredItems => (
          <VirtualList
            items={filteredItems}
            estimatedListItemHeight={48}
            renderListItem={renderListItem}
          />
        )}
      </FilterableList>
    )
  },
}

export const FilterableListWithCounter: Story = {
  render: () => {
    const items = generateMockItems(100)
    return (
      <FilterableList
        items={items}
        placeholder="Filter by name or description..."
        filterOptions={{
          keys: ['name', 'description'],
          threshold: 0.3,
        }}
      >
        {filteredItems => (
          <VirtualList
            items={filteredItems}
            estimatedListItemHeight={48}
            renderListItem={renderListItem}
          />
        )}
      </FilterableList>
    )
  },
}

export const FilterableListDisabled: Story = {
  render: () => {
    const items = generateMockItems(20)
    return (
      <div
        style={{
          height: '400px',
          border: '1px solid #e5e5e5',
          borderRadius: '8px',
        }}
      >
        <FilterableList
          items={items}
          disabled
          placeholder="Search is disabled"
          filterOptions={{
            keys: ['name', 'description'],
          }}
        >
          {filteredItems => (
            <VirtualList
              items={filteredItems}
              estimatedListItemHeight={60}
              renderListItem={(item: MockItem) => (
                <div
                  key={item.id}
                  style={{
                    padding: '12px 16px',
                    borderBottom: '1px solid #e5e5e5',
                    backgroundColor: '#fff',
                    opacity: 0.6,
                  }}
                >
                  <div style={{ fontWeight: 500 }}>{item.name}</div>
                  <div style={{ fontSize: '14px', color: '#666' }}>
                    {item.description}
                  </div>
                </div>
              )}
            />
          )}
        </FilterableList>
      </div>
    )
  },
}
