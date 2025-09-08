import type { Meta, StoryObj } from '@storybook/react-vite'
import React from 'react'

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
        afterInput={(filteredItems: MockItem[]) => (
          <FilterableList.Counter
            itemsLength={items.length}
            filteredItemsLength={filteredItems.length}
          />
        )}
      >
        {filteredItems =>
          filteredItems.length > 0 ? (
            <VirtualList
              items={filteredItems}
              estimatedListItemHeight={48}
              renderListItem={renderListItem}
            />
          ) : (
            <FilterableList.EmptyMessage message="No items match your search" />
          )
        }
      </FilterableList>
    )
  },
}

export const FilterableListWithCategories: Story = {
  render: () => {
    const items = generateMockItems(100)
    const [selectedCategory, setSelectedCategory] = React.useState<
      string | null
    >(null)

    const filteredByCategory = selectedCategory
      ? items.filter(item => item.category === selectedCategory)
      : items

    return (
      <div
        style={{
          height: '400px',
          border: '1px solid #e5e5e5',
          borderRadius: '8px',
        }}
      >
        <FilterableList
          items={filteredByCategory}
          placeholder="Search within category..."
          filterOptions={{
            keys: ['name', 'description'],
            threshold: 0.3,
          }}
          beforeInput={() => (
            <select
              value={selectedCategory || ''}
              onChange={e => setSelectedCategory(e.target.value || null)}
              style={{
                padding: '4px 8px',
                borderRadius: '4px',
                border: '1px solid #ddd',
                fontSize: '12px',
              }}
            >
              <option value="">All Categories</option>
              <option value="Documents">Documents</option>
              <option value="Images">Images</option>
              <option value="Videos">Videos</option>
              <option value="Audio">Audio</option>
              <option value="Other">Other</option>
            </select>
          )}
          afterInput={(filteredItems: MockItem[]) => (
            <FilterableList.Counter
              itemsLength={filteredByCategory.length}
              filteredItemsLength={filteredItems.length}
            />
          )}
        >
          {filteredItems =>
            filteredItems.length > 0 ? (
              <VirtualList
                items={filteredItems}
                estimatedListItemHeight={48}
                renderListItem={renderListItem}
              />
            ) : (
              <FilterableList.EmptyMessage />
            )
          }
        </FilterableList>
      </div>
    )
  },
}

export const FilterableListWithCustomActions: Story = {
  render: () => {
    const items = generateMockItems(30)
    const [sortOrder, setSortOrder] = React.useState<'asc' | 'desc'>('asc')

    const sortedItems = [...items].sort((a, b) => {
      return sortOrder === 'asc'
        ? a.name.localeCompare(b.name)
        : b.name.localeCompare(a.name)
    })

    return (
      <div
        style={{
          height: '400px',
          border: '1px solid #e5e5e5',
          borderRadius: '8px',
        }}
      >
        <FilterableList
          items={sortedItems}
          placeholder="Type to filter..."
          autoFocus
          filterOptions={{
            keys: ['name', 'description', 'category'],
            threshold: 0.3,
            includeScore: true,
          }}
          beforeInput={() => (
            <button
              onClick={() =>
                setSortOrder(prev => (prev === 'asc' ? 'desc' : 'asc'))
              }
              style={{
                padding: '4px 8px',
                borderRadius: '4px',
                border: '1px solid #ddd',
                backgroundColor: '#fff',
                cursor: 'pointer',
                fontSize: '12px',
                display: 'flex',
                alignItems: 'center',
                gap: '4px',
              }}
            >
              Sort {sortOrder === 'asc' ? '↑' : '↓'}
            </button>
          )}
          afterInput={(filteredItems: MockItem[]) => (
            <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
              <FilterableList.Counter
                itemsLength={sortedItems.length}
                filteredItemsLength={filteredItems.length}
              />
              {filteredItems.length > 0 && (
                <button
                  onClick={() => alert(`Export ${filteredItems.length} items`)}
                  style={{
                    padding: '4px 8px',
                    borderRadius: '4px',
                    border: '1px solid #4caf50',
                    backgroundColor: '#4caf50',
                    color: '#fff',
                    cursor: 'pointer',
                    fontSize: '11px',
                  }}
                >
                  Export
                </button>
              )}
            </div>
          )}
        >
          {(filteredItems, resetSearch) => (
            <>
              {filteredItems.length > 0 && (
                <div
                  style={{
                    padding: '8px 16px',
                    backgroundColor: '#f9f9f9',
                    borderBottom: '1px solid #e5e5e5',
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                  }}
                >
                  <span style={{ fontSize: '12px', color: '#666' }}>
                    Showing {filteredItems.length} results
                  </span>
                  {filteredItems.length < sortedItems.length && (
                    <button
                      onClick={resetSearch}
                      style={{
                        padding: '2px 8px',
                        fontSize: '11px',
                        border: 'none',
                        backgroundColor: 'transparent',
                        color: '#1976d2',
                        cursor: 'pointer',
                        textDecoration: 'underline',
                      }}
                    >
                      Clear filter
                    </button>
                  )}
                </div>
              )}
              {filteredItems.length > 0 ? (
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
                      }}
                    >
                      <div style={{ fontWeight: 500 }}>{item.name}</div>
                      <div style={{ fontSize: '14px', color: '#666' }}>
                        {item.description}
                      </div>
                    </div>
                  )}
                />
              ) : (
                <FilterableList.EmptyMessage message="No matching items found" />
              )}
            </>
          )}
        </FilterableList>
      </div>
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
