import Page from '../root/Page'

export default function PageTests(): JSX.Element {
  return (
    <Page
      sidebar={<div></div>}
      content={<div>Tests</div>}
    />
  )
}
