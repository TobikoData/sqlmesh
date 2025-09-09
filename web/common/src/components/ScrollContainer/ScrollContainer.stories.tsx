import { ScrollContainer, type ScrollContainerProps } from './ScrollContainer'

export default {
  title: 'Components/Containers/ScrollContainer',
  component: ScrollContainer,
}

const content = Array.from({ length: 30 }, (_, i) => (
  <div
    key={i}
    style={{ padding: 8, borderBottom: '1px solid #eee' }}
  >
    Row {i + 1}
  </div>
))

export const VerticalScroll = (args: ScrollContainerProps) => (
  <div style={{ height: 200, width: 300, border: '1px solid #ccc' }}>
    <ScrollContainer
      {...args}
      direction="vertical"
    >
      {content}
    </ScrollContainer>
  </div>
)

export const HorizontalScroll = (args: ScrollContainerProps) => (
  <div style={{ width: 300, border: '1px solid #ccc', overflow: 'hidden' }}>
    <ScrollContainer
      {...args}
      direction="horizontal"
    >
      <div style={{ display: 'flex' }}>
        {Array.from({ length: 10 }, (_, i) => (
          <span
            key={i}
            style={{
              display: 'inline-block',
              width: 150,
              padding: 8,
              borderRight: '1px solid #eee',
            }}
          >
            Column {i + 1}
          </span>
        ))}
      </div>
    </ScrollContainer>
  </div>
)

export const BothDirectionsScroll = (args: ScrollContainerProps) => (
  <div style={{ height: 200, width: 300, border: '1px solid #ccc' }}>
    <ScrollContainer
      {...args}
      direction="both"
    >
      <div style={{ width: 600 }}>
        {Array.from({ length: 30 }, (_, i) => (
          <div
            key={i}
            style={{
              padding: 8,
              borderBottom: '1px solid #eee',
              whiteSpace: 'nowrap',
            }}
          >
            Row {i + 1} - This is a long line of text that should cause
            horizontal scrolling when combined with the vertical scroll
          </div>
        ))}
      </div>
    </ScrollContainer>
  </div>
)

export const CustomClassName = (args: ScrollContainerProps) => (
  <div style={{ height: 200, width: 300 }}>
    <ScrollContainer
      {...args}
      className="bg-neutral-10 p-4 rounded-2xl"
    >
      {content}
    </ScrollContainer>
  </div>
)
CustomClassName.storyName = 'With Custom ClassName'

export const PageContentLayout = (args: ScrollContainerProps) => (
  <div style={{ height: '90vh', width: '100%', border: '1px solid #ccc' }}>
    <ScrollContainer {...args}>
      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-4 bg-neutral-10 p-4 rounded-2xl">
          Actions
        </div>
        <div
          className="flex flex-col gap-4 bg-neutral-10 p-4 rounded-2xl"
          style={{ height: '2000px' }}
        >
          Content
        </div>
        <div className="flex flex-col gap-4 bg-neutral-10 p-4 rounded-2xl">
          End
        </div>
      </div>
    </ScrollContainer>
  </div>
)
