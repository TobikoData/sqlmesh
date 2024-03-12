import SplitPane from '@components/splitPane/SplitPane'
import { useStoreContext } from '@context/context'
import { isNil } from '@utils/index'

export default function Page({
  sidebar,
  content,
}: {
  sidebar?: React.ReactNode
  content: React.ReactNode
}): JSX.Element {
  const splitPaneSizes = useStoreContext(s => s.splitPaneSizes)
  const setSplitPaneSizes = useStoreContext(s => s.setSplitPaneSizes)

  return (
    <>
      {isNil(sidebar) ? (
        <div className="flex w-full h-full overflow-hidden justify-center">
          {content}
        </div>
      ) : (
        <SplitPane
          sizes={splitPaneSizes}
          minSize={[0, 0]}
          snapOffset={0}
          className="flex w-full h-full overflow-hidden"
          onDragEnd={setSplitPaneSizes}
        >
          <div className="w-full h-full overflow-hidden">{sidebar}</div>
          <div className="w-full h-full overflow-hidden">{content}</div>
        </SplitPane>
      )}
    </>
  )
}
