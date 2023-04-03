export default function Footer(): JSX.Element {
  return (
    <footer className="px-2 py-1 text-xs flex justify-between">
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
