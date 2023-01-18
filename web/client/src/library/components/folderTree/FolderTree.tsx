import { useQueryClient } from '@tanstack/react-query';
import { useProjectStructure, type Folder, type File } from '../../../api';
import { FolderOpenIcon, DocumentIcon, } from '@heroicons/react/24/solid'
import { FolderIcon, DocumentIcon as DocumentIconOutline } from '@heroicons/react/24/outline'
import { ChevronRightIcon, ChevronDownIcon } from '@heroicons/react/20/solid'
import { useContext, useState } from 'react';
import Ide from '../../../context/Ide';
import clsx from 'clsx';

const CSS_ICON_SIZE = 'w-4 h-4';

export function FolderTree() {
  useQueryClient();

  const { setFile, file, files } = useContext(Ide);

  const { status, data } = useProjectStructure();

  if (status === 'loading') {
    return <h3>Loading...</h3>;
  }

  if (status === 'error') {
    return <h3>Error</h3>;
  }


  return (
    <div className='py-4 px-2 overflow-hidden'>
      {Boolean(data?.folders?.length) && (
        <Folders
          folders={data.folders}
          withIndent={false}
          selectFile={setFile}
          activeFile={file}
          activeFiles={files}
         />
      )}
      {Boolean(data?.files?.length) && (
        <Files
          files={data.files}
          selectFile={setFile}
          activeFile={file}
          activeFiles={files}
        />
      )}
    </div>
  );
}

function Folders(props: { folders: Folder[], withIndent: boolean, selectFile?: any, activeFile?: File, activeFiles?: Set<File> } = { folders: [], withIndent: false }) {
  return (
    <ul className={`${props.withIndent ? 'ml-4': '' } mr-1 overflow-hidden`}>
      {props.folders.map((folder) => (
        <li key={folder.id} title={folder.name}>
          <Folder
            folder={folder}
            selectFile={props.selectFile}
            activeFile={props.activeFile}
            activeFiles={props.activeFiles}
          />
        </li>
      ))}
    </ul>
  )
}

function Folder(props: { folder: Folder, selectFile?: any, activeFile?: File, activeFiles?: Set<File> }) {
  const [isOpen, setOpen] = useState(false);
  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon;
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon;

  return (
    <>
      <span className='text-base block whitespace-nowrap pb-1 cursor-pointer hover:bg-gray-800 group text-secondary-500 hover:text-gray-100' onClick={() => setOpen(!isOpen)}>
        <IconChevron className={`inline-block ${CSS_ICON_SIZE} mr-2`} />
        <IconFolder  className={`inline-block ${CSS_ICON_SIZE} mr-3`} />
        <span className='inline-block text-gray-800 group-hover:text-gray-100'>
          {props.folder.name}
        </span>
      </span>
      {isOpen && Array.isArray(props.folder.folders) && Boolean(props.folder.folders.length) && (
        <Folders
          folders={props.folder.folders}
          withIndent={true}
          selectFile={props.selectFile}
          activeFile={props.activeFile}
          activeFiles={props.activeFiles}
        />
      )}
      {isOpen && Array.isArray(props.folder.files) && Boolean(props.folder.files.length) && (
        <Files
          files={props.folder.files}
          selectFile={props.selectFile}
          activeFile={props.activeFile}
          activeFiles={props.activeFiles}
        />
      )}
    </>
  )
}

function Files(props: { files: File[], selectFile?: any, activeFile?: File, activeFiles?: Set<File>} 
  = { files: [] }) {
  return (
    <ul className='ml-4 mr-1 overflow-hidden'>
      {props.files.map((f) => (
        <li
          key={f.id}
          title={f.name}
          onClick={() => f.is_supported && props.selectFile(f)}
        >
          <span className={clsx(
            'text-base block whitespace-nowrap pb-1 ',
            !f.is_supported
              ? 'opacity-50 cursor-not-allowed'
              : 'group cursor-pointer hover:bg-gray-800 hover:text-gray-100',
            f.id === props.activeFile?.id ? ' text-secondary-500' : 'text-gray-800',
            
          )}>
            {props.activeFiles?.has(f) && (<DocumentIcon className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`} />)}
            {!props.activeFiles?.has(f) && (<DocumentIconOutline className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`} />)}            
            
            <span className='inline-block  group-hover:text-gray-100'>
              {f.name}
            </span>
          </span>
        </li>
      ))}
    </ul>
  )
}