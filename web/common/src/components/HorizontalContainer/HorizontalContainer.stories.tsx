import {
  HorizontalContainer,
  type HorizontalContainerProps,
} from './HorizontalContainer'

export default {
  title: 'Components/Containers/HorizontalContainer',
  component: HorizontalContainer,
}

const content = Array.from({ length: 20 }, (_, i) => (
  <div
    key={i}
    style={{
      minWidth: 100,
      padding: 8,
      borderRight: '1px solid #eee',
      display: 'inline-block',
    }}
  >
    Col {i + 1}
  </div>
))

export const Default = (args: HorizontalContainerProps) => (
  <div style={{ height: 350, width: '100%', border: '1px solid #ccc' }}>
    <HorizontalContainer
      {...args}
      scroll={false}
    >
      {content}
    </HorizontalContainer>
  </div>
)
Default.storyName = 'Default (No Scroll)'

export const WithScroll = (args: HorizontalContainerProps) => (
  <div style={{ height: 350, width: '100%', border: '1px solid #ccc' }}>
    <HorizontalContainer
      {...args}
      scroll={true}
    >
      <div style={{ width: 2000, display: 'flex' }}>{content}</div>
    </HorizontalContainer>
  </div>
)

export const CustomClassName = (args: HorizontalContainerProps) => (
  <div style={{ height: 350, width: '100%', border: '1px solid #ccc' }}>
    <HorizontalContainer
      {...args}
      className="bg-neutral-10 p-4 rounded-2xl"
    >
      {content}
    </HorizontalContainer>
  </div>
)
CustomClassName.storyName = 'With Custom ClassName'

export const NestedHorizontalContainer = (args: HorizontalContainerProps) => (
  <div style={{ height: 350, width: '100%', border: '1px solid #ccc' }}>
    <HorizontalContainer
      {...args}
      scroll={false}
      className="gap-4"
    >
      <div style={{ background: '#f5f5f5', padding: 8 }}>Left</div>
      <div
        style={{ width: '100%', border: '1px solid #eee', overflow: 'hidden' }}
      >
        <HorizontalContainer scroll={true}>
          <div style={{ width: 2000, display: 'flex' }}>{content}</div>
        </HorizontalContainer>
      </div>
      <div style={{ background: '#f5f5f5', padding: 8, flexShrink: 0 }}>
        Right
      </div>
    </HorizontalContainer>
  </div>
)
NestedHorizontalContainer.storyName = 'Nested HorizontalContainer'
