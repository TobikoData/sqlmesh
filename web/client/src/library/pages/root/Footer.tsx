import { useStoreContext } from '@context/context'
import { isNotNil } from '@utils/index'

export default function Footer(): JSX.Element {
  const version = useStoreContext(s => s.version)

  return (
    <footer className="px-2 py-1 text-xs flex justify-between">
      <span>
        {isNotNil(version) && (
          <>
            <span className="hidden sm:inline">SQLMesh: </span>
            <span
              title="SQLMesh Version"
              className="font-black inline-block mr-4"
            >
              {version}
            </span>
          </>
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
