import type { Meta, StoryObj } from '@storybook/react-vite'
import { Description } from './Description'
import { Headline } from './Headline'
import { Information } from './Information'
import { Tagline } from './Tagline'
import { Text } from './Text'
import { TextBlock } from './TextBlock'

const meta = {
  title: 'Typography',
  decorators: [
    Story => (
      <div className="p-8 space-y-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta

export default meta

export const AllTypography: StoryObj = {
  render: () => (
    <div className="space-y-12">
      <section>
        <h2 className="text-2xl font-bold mb-6 text-gray-800 dark:text-gray-200">
          Headlines
        </h2>
        <div className="space-y-4">
          <Headline level={1}>Headline Level 1</Headline>
          <Headline level={2}>Headline Level 2</Headline>
          <Headline level={3}>Headline Level 3</Headline>
          <Headline level={4}>Headline Level 4</Headline>
          <Headline level={5}>Headline Level 5</Headline>
          <Headline level={6}>Headline Level 6</Headline>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold mb-6 text-gray-800 dark:text-gray-200">
          Text Components
        </h2>
        <div className="space-y-4">
          <div>
            <h3 className="text-lg font-semibold mb-2">Text</h3>
            <Text>
              This is a regular text component with prose styling. It handles
              paragraph text and maintains proper whitespace wrapping.
            </Text>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-2">Description</h3>
            <Description>
              This is a description text component using smaller text size and
              typography-description color.
            </Description>
          </div>

          <div>
            <h3 className="text-lg font-semibold mb-2">Tagline</h3>
            <Tagline>
              This is a tagline component with extra small text and ellipsis
              overflow handling
            </Tagline>
          </div>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold mb-6 text-gray-800 dark:text-gray-200">
          Information Component
        </h2>
        <div className="space-y-4">
          <Information
            info="This is a tooltip that appears when you hover over the info icon"
            size="s"
          >
            <span>Text with information tooltip (small)</span>
          </Information>

          <Information
            info="Medium sized tooltip with more detailed information about this particular item"
            size="m"
          >
            <span>Text with information tooltip (medium)</span>
          </Information>

          <Information
            info="Large tooltip text that can contain even more detailed explanations"
            size="l"
            side="left"
          >
            <span>Text with information tooltip (large, left side)</span>
          </Information>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold mb-6 text-gray-800 dark:text-gray-200">
          TextBlock Component
        </h2>
        <div className="space-y-6">
          <TextBlock
            level={2}
            headline="TextBlock with Headline"
            tagline="Supporting tagline text"
          >
            <Text>
              This is the body content of the TextBlock component. It combines a
              headline, optional tagline, and children content.
            </Text>
          </TextBlock>

          <TextBlock
            level={3}
            headline="TextBlock with Info"
            tagline="Hover the info icon for details"
            info="This TextBlock includes an information tooltip that provides additional context about the content."
          >
            <Description>
              The TextBlock component is a composite that brings together
              multiple typography elements.
            </Description>
          </TextBlock>

          <TextBlock
            level={4}
            headline="Simple TextBlock"
          >
            <Text>A minimal TextBlock with just a headline and content.</Text>
          </TextBlock>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold mb-6 text-gray-800 dark:text-gray-200">
          Typography Combinations
        </h2>
        <div className="space-y-6">
          <div className="border border-gray-200 dark:border-gray-700 rounded p-4">
            <Headline level={2}>Article Title</Headline>
            <Tagline className="mt-2">Published on December 8, 2024</Tagline>
            <Text className="mt-4">
              Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do
              eiusmod tempor incididunt ut labore et dolore magna aliqua.
            </Text>
            <Description className="mt-2">Read time: 5 minutes</Description>
          </div>

          <div className="border border-gray-200 dark:border-gray-700 rounded p-4">
            <Information
              info="Click to learn more about this feature"
              size="m"
            >
              <Headline level={3}>Feature Card</Headline>
            </Information>
            <Description className="mt-2">
              A powerful feature that combines multiple capabilities into one
              seamless experience.
            </Description>
          </div>
        </div>
      </section>

      <section>
        <h2 className="text-2xl font-bold mb-6 text-gray-800 dark:text-gray-200">
          Custom Styling
        </h2>
        <div className="space-y-4">
          <Headline
            level={2}
            className="text-blue-600 dark:text-blue-400"
          >
            Custom Blue Headline
          </Headline>
          <Text className="text-green-600 dark:text-green-400 italic">
            Custom styled text with italic green color
          </Text>
          <Description className="text-purple-600 dark:text-purple-400 font-bold">
            Bold purple description text
          </Description>
          <Tagline className="text-orange-600 dark:text-orange-400">
            Orange tagline with custom color
          </Tagline>
        </div>
      </section>
    </div>
  ),
}

export const Headlines: StoryObj = {
  render: () => (
    <div className="space-y-4">
      <Headline level={1}>Headline Level 1 - Bold and Large</Headline>
      <Headline level={2}>
        Headline Level 2 - Semibold and Slightly Smaller
      </Headline>
      <Headline level={3}>Headline Level 3 - Medium Weight</Headline>
      <Headline level={4}>Headline Level 4 - Smaller Size</Headline>
      <Headline level={5}>Headline Level 5 - Compact</Headline>
      <Headline level={6}>Headline Level 6 - Smallest</Headline>
    </div>
  ),
}

export const TextComponents: StoryObj = {
  render: () => (
    <div className="space-y-6">
      <div>
        <h3 className="font-semibold mb-2">Text Component</h3>
        <Text>Regular paragraph text with prose styling for readability.</Text>
      </div>
      <div>
        <h3 className="font-semibold mb-2">Description Component</h3>
        <Description>
          Smaller text used for descriptions and secondary information.
        </Description>
      </div>
      <div>
        <h3 className="font-semibold mb-2">Tagline Component</h3>
        <Tagline>
          Brief tagline text with ellipsis overflow handling for long content
          that extends beyond the available space
        </Tagline>
      </div>
    </div>
  ),
}

export const InformationTooltips: StoryObj = {
  render: () => (
    <div className="space-y-6">
      <Information
        info="Extra small tooltip"
        size="xs"
      >
        <span>Hover for XS tooltip</span>
      </Information>
      <Information
        info="Small tooltip with information"
        size="s"
      >
        <span>Hover for Small tooltip</span>
      </Information>
      <Information
        info="Medium sized tooltip with more detailed information"
        size="m"
      >
        <span>Hover for Medium tooltip</span>
      </Information>
      <Information
        info="Large tooltip that can contain extensive information and explanations"
        size="l"
      >
        <span>Hover for Large tooltip</span>
      </Information>
      <Information
        info="This tooltip appears on the left side"
        side="left"
        size="m"
      >
        <span>Left-aligned tooltip</span>
      </Information>
    </div>
  ),
}

export const TextBlocks: StoryObj = {
  render: () => (
    <div className="space-y-8">
      <TextBlock
        level={1}
        headline="Complete TextBlock"
        tagline="With all properties"
        info="This is a complete TextBlock with headline, tagline, info, and content"
      >
        <Text>
          This TextBlock demonstrates all available properties including the
          information tooltip.
        </Text>
      </TextBlock>

      <TextBlock
        level={2}
        headline="TextBlock without Info"
        tagline="Just headline and tagline"
      >
        <Description>
          A simpler TextBlock without the information tooltip.
        </Description>
      </TextBlock>

      <TextBlock
        level={3}
        headline="Minimal TextBlock"
      >
        <Text>Only headline and content, no tagline or info.</Text>
      </TextBlock>
    </div>
  ),
}
