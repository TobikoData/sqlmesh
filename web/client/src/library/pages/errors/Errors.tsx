import { useIDE } from '../ide/context'
import Page from '../root/Page'

export default function PageErrors(): JSX.Element {
  const { errors } = useIDE()

  return (
    <Page
      sidebar={
        <div>
          {Array.from(errors).map((error, index) => (
            <div key={index}>{error.message}</div>
          ))}
        </div>
      }
      content={<div></div>}
    />
  )
}
