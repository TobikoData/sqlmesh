import { type Directory, type File } from '../../../api/endpoints';
import { FolderOpenIcon, DocumentIcon, FolderPlusIcon, DocumentPlusIcon, XCircleIcon } from '@heroicons/react/24/solid'
import { FolderIcon, DocumentIcon as DocumentIconOutline } from '@heroicons/react/24/outline'
import { ChevronRightIcon, ChevronDownIcon } from '@heroicons/react/20/solid'
import { useContext, useEffect, useState } from 'react';
import ContextIDE from '../../../context/Ide';
import clsx from 'clsx';
import {  toSingular } from '../../../utils';

/* TODO:
  - connect to API
  - add ability to create file or directory on top level
  - add context menu
  - add confirmation before delete
  - add rename
  - add drag and drop
  - add copy and paste
  - add move
  - add search
*/

const CSS_ICON_SIZE = 'w-4 h-4';
class Counter {
  private store = new Map<string, number>();

  countByKey(key: string | string[]): void {
    key = Counter.toKey(key);

    const count = (this.store.get(key) ?? 0) + 1;

    this.store.set(key, count);
  }

  getCountByKey(key: string | string[]): number {
    key = Counter.toKey(key);

    return this.store.get(key) ?? 0;
  }

  static toKey(key: string | string[]): string {
    return Array.isArray(key) ? key.join('_') : key;
  }  
}

const counter = new Counter()

export function FolderTree({ project }: { project: any }) {
  const { setActiveFile, activeFile, openedFiles, setOpenedFiles } = useContext(ContextIDE);
  const [directory, setDirectory] = useState<Directory>(project);

  useEffect(() => {
    setDirectory(project)
  }, [project])

  function create(e: any, type: 'file' | 'directory', parent: Directory, extension = '.py'): boolean {
    e.stopPropagation()

    if (type === 'directory') {
      counter.countByKey([type, parent.path])

      const name = `new_directory_${counter.getCountByKey([type, parent.path])}`.toLowerCase()

      parent.directories.push({
        name,
        path: parent.path + `/${name}`,
        files: [],
        directories: []
      })
    } else {
      counter.countByKey([type, parent.path])

      const count = counter.getCountByKey([type, parent.path])
      const name = parent.name.startsWith('new_')
        ? `new_file_${count}${extension}`
        : `new_${toSingular(parent.name)}_${count}${extension}`.toLowerCase()

      parent.files.push({
        name,
        extension,
        path: parent.path + `/${name}.${extension}`,
        content: '',
        is_supported: true,
      })
    }

    setDirectory({ ...directory })

    return true
  }

  function remove(e: any, artifact: Artifact, parent: Directory): boolean {
    e.stopPropagation()

    if ('extension' in artifact) {
      parent.files = parent.files.filter(f => f.path !== artifact.path)

      if (openedFiles.delete(artifact)) {
        setOpenedFiles(new Set(openedFiles))
      }

      if (activeFile?.path === artifact.path) {
        setActiveFile(null)
      }

    } else {
      const files = getAllFilesInDirectory(artifact)

      files.forEach(file => {
        openedFiles.delete(file)
      })

      files.forEach(file => {
        if (activeFile?.path === file.path) {
          setActiveFile([...openedFiles][0])
        }
      })

      parent.directories = parent.directories.filter(f => f.path !== artifact.path)

      setOpenedFiles(new Set(openedFiles))
    }

    setDirectory({ ...directory })

    return true
  }

  function getAllFilesInDirectory(dir: Directory): File[] {
    const files = dir.files || []
    const directories = dir.directories || []

    return [
      ...files,
      ...directories.map(directory => getAllFilesInDirectory(directory)).flat()
    ]
  }

  return (
    <div className='py-2 overflow-hidden'>
      {Boolean(directory?.directories?.length) && (
        <Directories
          directories={directory.directories}
          withIndent={false}
          selectFile={setActiveFile}
          activeFile={activeFile}
          activeFiles={openedFiles}
          parent={directory}
          create={create}
          remove={remove}
        />
      )}
      {Boolean(directory?.files?.length) && (
        <Files
          files={directory.files}
          selectFile={setActiveFile}
          activeFile={activeFile}
          activeFiles={openedFiles}
          parent={directory}
          create={create}
          remove={remove}
        />
      )}
    </div>
  );
}

type Artifact = File | Directory

interface PropsArtifacts {
  activeFile: File | null;
  activeFiles?: Set<File>;
  selectFile: (file: File) => void;
  parent: Directory;
  remove: (e: MouseEvent, file: Artifact, parent: Directory) => boolean;
  create: (e: MouseEvent, type: 'file' | 'directory', parent: Directory, extension?: '.py' | '.yaml' | '.sql') => boolean;
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

function Directories({ directories = [], withIndent = false, selectFile, activeFile, activeFiles, create, remove, parent }: PropsDirectories) {
  return (
    <ul className={`${withIndent ? 'ml-4' : ''} overflow-hidden`}>
      {directories.map(directory => (
        <li key={directory.path} title={directory.name} className='border-l px-1'>
          <Directory
            directory={directory}
            selectFile={selectFile}
            activeFile={activeFile}
            activeFiles={activeFiles}
            parent={parent}
            create={create}
            remove={remove}            
          />
        </li>
      ))}
    </ul>
  )
}

function Directory({ directory, selectFile, activeFile, activeFiles, parent, create, remove }: PropsDirectory) {
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
          <DocumentPlusIcon onClick={(e: any) => create(e, "file", directory) && setOpen(true)}  className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`} />
          <FolderPlusIcon onClick={(e: any) => create(e, "directory", directory) && setOpen(true)} className={`cursor-pointer inline-block ${CSS_ICON_SIZE} mr-1 text-secondary-300 hover:text-secondary-500`} />
          <XCircleIcon onClick={(e: any) => remove(e, directory, parent)} className={`cursor-pointer inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500`} />
        </span>
      </span>
      {isOpen && withFolders && (
        <Directories
          directories={directory.directories}
          withIndent={true}
          selectFile={selectFile}
          activeFile={activeFile}
          activeFiles={activeFiles}
          parent={directory}
          create={create}
          remove={remove}          
        />
      )}
      {isOpen && withFiles && (
        <Files
          files={directory.files}
          selectFile={selectFile}
          activeFile={activeFile}
          activeFiles={activeFiles}
          parent={directory}
          create={create}
          remove={remove} 
        />
      )}
    </>
  )
}

function Files({ files = [], activeFiles, activeFile, selectFile, remove, parent }: PropsFiles) {
  return (
    <ul className='ml-4 mr-1 overflow-hidden'>
      {files.map(file => (
        <li
          key={file.path}
          title={file.name}
          onClick={() => file.is_supported && file !== activeFile && selectFile(file)}
          className={'border-l px-1'}
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

            <span className='invisible group-hover/file:visible min-w-8' onClick={(e: any) => remove(e, file, parent)}>
              <XCircleIcon className={`inline-block ${CSS_ICON_SIZE} ml-2 text-danger-300 hover:text-danger-500 cursor-pointer`} />
            </span>
          </span>
        </li>
      ))}
    </ul>
  )
}

