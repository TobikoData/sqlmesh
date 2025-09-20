import type { Meta, StoryObj } from '@storybook/react-vite'
import { Description as DescriptionComponent } from './Description'
import { Headline } from './Headline'
import { Information as InformationComponent } from './Information'
import { Tagline as TaglineComponent } from './Tagline'
import { Text as TextComponent } from './Text'

export default {
  title: 'Components/Typography',
  decorators: [
    Story => (
      <div className="p-8 space-y-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta

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

export const Text: StoryObj = {
  render: () => (
    <TextComponent>
      Regular paragraph text with prose styling for readability.
    </TextComponent>
  ),
}

export const Tagline: StoryObj = {
  render: () => (
    <TaglineComponent>
      Tagline text with prose styling for readability.
    </TaglineComponent>
  ),
}

export const Description: StoryObj = {
  render: () => (
    <DescriptionComponent>
      Description text with prose styling for readability.
    </DescriptionComponent>
  ),
}

export const Information: StoryObj = {
  render: () => (
    <div className="space-y-6">
      <InformationComponent
        info="Extra small tooltip"
        size="xs"
      >
        <span>Hover for XS tooltip</span>
      </InformationComponent>
      <InformationComponent
        info="Small tooltip with information"
        size="s"
      >
        <span>Hover for Small tooltip</span>
      </InformationComponent>
      <InformationComponent
        info="Medium sized tooltip with more detailed information"
        size="m"
      >
        <span>Hover for Medium tooltip</span>
      </InformationComponent>
      <InformationComponent
        info="Large tooltip that can contain extensive information and explanations"
        size="l"
      >
        <span>Hover for Large tooltip</span>
      </InformationComponent>
    </div>
  ),
}
