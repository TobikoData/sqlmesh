import { useStoreContext } from '@context/context'
import { isArrayNotEmpty, isNotNil } from '@utils/index'

export default function Footer(): JSX.Element {
  const version = useStoreContext(s => s.version)
  const modules = useStoreContext(s => s.modules)

  return (
    <footer className="px-2 py-1 text-xs flex justify-between">
      <span>
        {isNotNil(version) && (
          <>
            <span>SQLMesh Version: </span>
            <span className="font-black inline-block mr-4">{version}</span>
          </>
        )}
        {isArrayNotEmpty(modules) && (
          <span className="capitalize inline-block mr-4">
            Modules: {modules.join(', ')}
          </span>
        )}
      </span>
      <small className="text-xs">
        © {new Date().getFullYear()}
        &nbsp;
        <a
          href="https://tobikodata.com/"
          target="_blank"
          rel="noopener noreferrer"
          className="underline"
          title="Tobiko Data website"
        >
          Tobiko&nbsp;Data,&nbsp;Inc.
        </a>
        &nbsp; All rights reserved.
      </small>
    </footer>
  )
}
