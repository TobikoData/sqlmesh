import { useQueryClient } from '@tanstack/react-query';
import { useApiFiles, type Directory, type File } from '../../../api';
import { FolderOpenIcon, DocumentIcon, FolderPlusIcon, DocumentPlusIcon, XCircleIcon } from '@heroicons/react/24/solid'
import { FolderIcon, DocumentIcon as DocumentIconOutline } from '@heroicons/react/24/outline'
import { ChevronRightIcon, ChevronDownIcon } from '@heroicons/react/20/solid'
import { useContext, useState } from 'react';
import Ide from '../../../context/Ide';
import clsx from 'clsx';

const CSS_ICON_SIZE = 'w-4 h-4';

export function FolderTree() {
  useQueryClient();

  const { setFile, file, files } = useContext(Ide);

  const { status, data } = useApiFiles();

  if (status === 'loading') {
    return <h3>Loading...</h3>;
  }

  if (status === 'error') {
    return <h3>Error</h3>;
  }


  return (
    <div className='py-4 px-2 overflow-hidden'>
      {Boolean(data?.directories?.length) && (
        <Directories
        directories={data.directories}
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

interface PropsDirectories {
  directories: Directory[];
  withIndent: boolean;
  selectFile?: (file: File) => void;
  activeFile?: File;
  activeFiles?: Set<File>;
}

interface PropsDirectory {
  directory: Directory;
  selectFile?: (file: File) => void;
  activeFile?: File;
  activeFiles?: Set<File>;
}

function Directories({ directories = [], withIndent = false, selectFile, activeFile, activeFiles }: PropsDirectories) {
  return (
    <ul className={`${withIndent ? 'ml-4': '' } mr-1 overflow-hidden`}>
      {directories.map(directory => (
        <li key={directory.path} title={directory.name}>
          <Directory
            directory={directory}
            selectFile={selectFile}
            activeFile={activeFile}
            activeFiles={activeFiles}
          />
        </li>
      ))}
    </ul>
  )
}

function Directory({ directory, selectFile, activeFile, activeFiles }: PropsDirectory) {
  const [isOpen, setOpen] = useState(false);
  const IconChevron = isOpen ? ChevronDownIcon : ChevronRightIcon;
  const IconFolder = isOpen ? FolderOpenIcon : FolderIcon;
  const withFolders = Array.isArray(directory.directories) && Boolean(directory.directories.length)
  const withFiles = Array.isArray(directory.files) && Boolean(directory.files.length)

  return (
    <>
      <span
        className='text-base whitespace-nowrap pb-1 px-2 hover:bg-secondary-100 group flex justify-between rounded-md'
      >
        <span>
          <IconChevron className={clsx(
            `inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500 cursor-pointer`,
            { 'invisible pointer-events-none cursor-default': !withFolders && !withFiles }
          )} onClick={() => setOpen(!isOpen)} />
          <span className='cursor-pointer' onClick={() => setOpen(!isOpen)}>
            <IconFolder className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500`} />
            <p className='inline-block font-light text-gray-600 group-hover:text-secondary-500'>
              {directory.name}
            </p>
          </span>
        </span>

        <span className='hidden group-hover:block'>
            <DocumentPlusIcon className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`} />
            <FolderPlusIcon  className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`} />
            <XCircleIcon  className={`inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500 cursor-pointer`} />
          </span>
      </span>
      {isOpen && withFolders && (
        <Directories
          directories={directory.directories}
          withIndent={true}
          selectFile={selectFile}
          activeFile={activeFile}
          activeFiles={activeFiles}
        />
      )}
      {isOpen && withFiles && (
        <Files
          files={directory.files}
          selectFile={selectFile}
          activeFile={activeFile}
          activeFiles={activeFiles}
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
          key={f.path}
          title={f.name}
          onClick={() => f.is_supported && props.selectFile(f)}
        >
          <span className={clsx(
            'text-base whitespace-nowrap pb-1 group/file px-2 flex justify-between rounded-md',
            f.path === props.activeFile?.path ? 'text-secondary-500' : 'text-gray-800',
            f.is_supported && 'group cursor-pointer hover:bg-secondary-100',
          )}>
            <span className={clsx(
              'flex w-full items-center overflow-hidden overflow-ellipsis',
              !f.is_supported && 'opacity-50 cursor-not-allowed text-gray-800',
            )}>
              {props.activeFiles?.has(f) && (<DocumentIcon className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`} />)}
              {!props.activeFiles?.has(f) && (<DocumentIconOutline className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`} />)}

              <span className='w-full overflow-hidden overflow-ellipsis group-hover:text-secondary-500'>
                {f.name}
              </span>              
            </span>

            <span className='invisible group-hover/file:visible min-w-8'>
              <XCircleIcon  className={`inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500 cursor-pointer`} />
            </span>
          </span>
        </li>
      ))}
    </ul>
  )
}