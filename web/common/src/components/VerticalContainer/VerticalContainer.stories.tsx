import {
  VerticalContainer,
  type VerticalContainerProps,
} from './VerticalContainer'

export default {
  title: 'Components/Containers/VerticalContainer',
  component: VerticalContainer,
}

const content = Array.from({ length: 20 }, (_, i) => (
  <div
    key={i}
    style={{ padding: 8, borderBottom: '1px solid #eee' }}
  >
    Row {i + 1}
  </div>
))

export const Default = (args: VerticalContainerProps) => (
  <div style={{ height: 350, width: '100%', border: '1px solid #ccc' }}>
    <VerticalContainer
      {...args}
      scroll={false}
    >
      {content}
    </VerticalContainer>
  </div>
)
Default.storyName = 'Default (No Scroll)'

export const WithScroll = (args: VerticalContainerProps) => (
  <div style={{ height: 350, width: '100%', border: '1px solid #ccc' }}>
    <VerticalContainer
      {...args}
      scroll={true}
    >
      {content}
    </VerticalContainer>
  </div>
)

export const CustomClassName = (args: VerticalContainerProps) => (
  <div style={{ height: 350, width: '100%' }}>
    <VerticalContainer
      {...args}
      className="bg-neutral-10 p-4 rounded-2xl"
    >
      {content}
    </VerticalContainer>
  </div>
)
CustomClassName.storyName = 'With Custom ClassName'

export const NestedVerticalContainer = (args: VerticalContainerProps) => (
  <div
    style={{
      height: 500,
      width: '100%',
      border: '1px solid #ccc',
      overflow: 'hidden',
    }}
  >
    <VerticalContainer
      {...args}
      scroll={false}
      className="gap-4"
    >
      <div style={{ background: '#f5f5f5', padding: 8 }}>Header</div>
      <div
        style={{ height: '100%', border: '1px solid #eee', overflow: 'hidden' }}
      >
        <VerticalContainer scroll={true}>{content}</VerticalContainer>
      </div>
      <div style={{ background: '#f5f5f5', padding: 8, flexShrink: 0 }}>
        Footer
      </div>
    </VerticalContainer>
  </div>
)
NestedVerticalContainer.storyName = 'Nested VerticalContainer'
