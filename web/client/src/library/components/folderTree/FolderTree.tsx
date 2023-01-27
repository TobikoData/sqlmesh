import { type Directory, type File } from '../../../api/endpoints';
import { FolderOpenIcon, DocumentIcon, FolderPlusIcon, DocumentPlusIcon, XCircleIcon } from '@heroicons/react/24/solid'
import { FolderIcon, DocumentIcon as DocumentIconOutline } from '@heroicons/react/24/outline'
import { ChevronRightIcon, ChevronDownIcon } from '@heroicons/react/20/solid'
import { useContext, useState } from 'react';
import ContextIDE from '../../../context/Ide';
import clsx from 'clsx';

const CSS_ICON_SIZE = 'w-4 h-4';

export function FolderTree({ project }: { project: any }) {
  const { setActiveFile, activeFile, openedFiles } = useContext(ContextIDE);

  return (
    <div className='py-2 overflow-hidden'>
      {Boolean(project?.directories?.length) && (
        <Directories
          directories={project.directories}
          withIndent={false}
          selectFile={setActiveFile}
          activeFile={activeFile}
          activeFiles={openedFiles}
        />
      )}
      {Boolean(project?.files?.length) && (
        <Files
          files={project.files}
          selectFile={setActiveFile}
          activeFile={activeFile}
          activeFiles={openedFiles}
        />
      )}
    </div>
  );
}

interface PropsArtifacts {
  activeFile: File | null;
  activeFiles?: Set<File>;
  selectFile: (file: File) => void;
}

interface PropsDirectories extends PropsArtifacts {
  directories: Directory[];
  withIndent: boolean;
}

interface PropsDirectory extends PropsArtifacts {
  directory: Directory;
}

interface PropsFiles extends PropsArtifacts {
  files: File[];
}

function Directories({ directories = [], withIndent = false, selectFile, activeFile, activeFiles }: PropsDirectories) {
  return (
    <ul className={`${withIndent ? 'ml-4' : ''} overflow-hidden`}>
      {directories.map(directory => (
        <li key={directory.path} title={directory.name} className='border-l px-1'>
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
        className='text-base whitespace-nowrap px-2 hover:bg-secondary-100 group flex justify-between rounded-md'
      >
        <span>
          <IconChevron className={clsx(
            `inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500 cursor-pointer`,
            { 'invisible pointer-events-none cursor-default': !withFolders && !withFiles }
          )} onClick={() => setOpen(!isOpen)} />
          <span className='cursor-pointer' onClick={() => setOpen(!isOpen)}>
            <IconFolder className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-500`} />
            <p className='inline-block text-sm ml-1 text-gray-900 group-hover:text-secondary-500'>
              {directory.name}
            </p>
          </span>
        </span>

        <span className='hidden group-hover:block'>
          <DocumentPlusIcon className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`} />
          <FolderPlusIcon className={`inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`} />
          <XCircleIcon className={`inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500 cursor-pointer`} />
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

function Files({ files = [], activeFiles, activeFile, selectFile, }: PropsFiles) {
  return (
    <ul className='ml-4 mr-1 overflow-hidden'>
      {files.map(file => (
        <li
          key={file.path}
          title={file.name}
          onClick={() => file.is_supported && file !== activeFile && selectFile(file)}
          className='border-l px-1'
        >
          <span className={clsx(
            'text-base whitespace-nowrap group/file px-2 flex justify-between rounded-md',
            file.path === activeFile?.path ? 'text-secondary-500' : 'text-gray-800',
            file.is_supported && 'group cursor-pointer hover:bg-secondary-100',
          )}>
            <span className={clsx(
              'flex w-full items-center overflow-hidden overflow-ellipsis',
              !file.is_supported && 'opacity-50 cursor-not-allowed text-gray-800',
            )}>
              {activeFiles?.has(file) && (<DocumentIcon className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`} />)}
              {!activeFiles?.has(file) && (<DocumentIconOutline className={`inline-block ${CSS_ICON_SIZE} mr-3 text-secondary-500`} />)}

              <span className='w-full text-sm overflow-hidden overflow-ellipsis group-hover:text-secondary-500'>
                {file.name}
              </span>
            </span>

            <span className='invisible group-hover/file:visible min-w-8'>
              <XCircleIcon className={`inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500 cursor-pointer`} />
            </span>
          </span>
        </li>
      ))}
    </ul>
  )
}