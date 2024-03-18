import Spinner from '@components/logo/Spinner'
import Loading from './Loading'

export default function LoadingStatus({
  children,
}: {
  children: React.ReactNode
}): JSX.Element {
  return (
    <Loading className="inline-block">
      <Spinner className="w-3 h-3 border border-neutral-10 mr-2" />
      <span className="inline-block text-xs whitespace-nowrap">{children}</span>
    </Loading>
  )
}
