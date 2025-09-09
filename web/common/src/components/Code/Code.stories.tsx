import type { Meta, StoryObj } from '@storybook/react-vite'
import { expect, within, userEvent, fn } from 'storybook/test'
import { Code } from './Code'

const meta: Meta<typeof Code> = {
  title: 'Components/Code',
  component: Code,
}

export default meta

type Story = StoryObj<typeof Code>

export const Default: Story = {
  args: {
    content: `MODEL (
  name tier1.compliance_application_affiliated_person,
  owner 'data-infra',
  start '2023-01-01',
  cron '0 */4 * * *',
  tags (creditrisk, tier1_mart),
  dialect bigquery,
  kind VIEW (
    materialized FALSE
  ),
  virtual_environment_mode 'full'
)
SELECT
  compliance_application_affiliated_person_internal_id,
  compliance_application_affiliated_person_id,
  effective_at,
  updated_at,
  business_affiliated_person_internal_id,
  status,
  address_city,
  address_country,
  address_line1,
  address_line2,
  address_post_code,
  address_state,
  birth_date,
  email,
  first_name,
  ip_address,
  last_name,
  middle_name,
  phone_nr,
  ssn,
  tags,
  alloy_entity_external_id,
  compliance_application_internal_id,
  is_ubo,
  is_control_person,
  datastream_metadata__uuid,
  datastream_metadata__source_timestamp
FROM tier1.stg_pg__compliance_application_affiliated_persons
    `,
  },
}
