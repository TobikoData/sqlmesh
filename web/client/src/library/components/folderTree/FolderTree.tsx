import { useQueryClient } from '@tanstack/react-query';
import { useProjectStructure, type Folder, type File } from '../../../api';
import { FolderOpenIcon, DocumentIcon } from '@heroicons/react/24/solid'
import { FolderIcon } from '@heroicons/react/24/outline'
import { ChevronRightIcon, ChevronDownIcon } from '@heroicons/react/20/solid'
import { useState } from 'react';

const CSS_ICON_SIZE = 'w-5 h-5';

export default function FolderTree() {
  useQueryClient();

  const { status, data } = useProjectStructure();

  if (status === 'loading') {
    return <h3>Loading...</h3>;
  }

  if (status === 'error') {
    return <h3>Error</h3>;
  }


  return (
    <div className='py-4 px-2 overflow-hidden'>
      {data?.folders?.length && <Folders folders={data.folders} withIndent={false} />}
      {data?.files?.length && <Files files={data.files}/>}
    </div>
  );
}

function Folders(props: { folders: Folder[], withIndent: boolean } = { folders: [], withIndent: false }) {
  return (
    <ul className={`${props.withIndent ? 'ml-4': '' } mr-1 overflow-hidden`}>
      {props.folders.map((folder) => (
        <li key={folder.id} title={folder.name}>
          <Folder folder={folder} />
        </li>
      ))}
    </ul>
  )
}

function Folder(props: { folder: Folder }) {
  const [isOpen, setOpen] = useState(false);
  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon;
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon;

  return (
    <>
      <span className='text-base block whitespace-nowrap pb-1 cursor-pointer hover:bg-gray-800' onClick={() => setOpen(!isOpen)}>
        <IconChevron className={`inline-block ${CSS_ICON_SIZE} mr-2 text-gray-100`} />
        <IconFolder  className={`inline-block ${CSS_ICON_SIZE} mr-3 text-gray-100`} />
        <span className='inline-block'>
          {props.folder.name}
        </span>
      </span>
      {isOpen && Array.isArray(props.folder.folders) && Boolean(props.folder.folders.length) && <Folders folders={props.folder.folders} withIndent={true} />}
      {isOpen && Array.isArray(props.folder.files) && Boolean(props.folder.files.length) && <Files files={props.folder.files}/>}
    </>
  )
}

function Files(props: { files: File[] } = { files: [] }) {
  return (
    <ul className='ml-4 mr-1 overflow-hidden'>
      {props.files.map((file) => (
        <li key={file.id} title={file.name}>
          <span className='text-base block whitespace-nowrap pb-1 cursor-pointer hover:bg-gray-800'>
            <DocumentIcon className={`inline-block ${CSS_ICON_SIZE} mr-3 text-gray-100`} />
            <span className='inline-block'>
              {file.name}
            </span>
          </span>
        </li>
      ))}
    </ul>
  )
}