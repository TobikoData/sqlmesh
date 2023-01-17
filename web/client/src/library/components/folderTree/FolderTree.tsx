import { useQueryClient } from '@tanstack/react-query';
import { useProjectStructure, type Folder, type File } from '../../../api';


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
    <div>
      {data?.folders && <Folders folders={data.folders} isOpen={true} />}
      {data?.files && <Files files={data.files}/>}
    </div>
  );
}

function Folders(props: { folders: Folder[], isOpen: boolean } = { folders: [], isOpen: true }) {
  return (
    <ul className='mx-4'>
      {props.folders.map((folder) => (
        <li key={folder.id} className='p-1'>
          {folder.name}
          {props.isOpen && Array.isArray(folder.folders) && <Folders folders={folder.folders} isOpen={false} />}
          {props.isOpen && Array.isArray(folder.files) && <Files files={folder.files}/>}
        </li>
      ))}
    </ul>
  )
}

function Files(props: { files: File[] } = { files: [] }) {
  return (
    <ul className='mx-4'>
      {props.files.map((file) => (
        <li key={file.id} className='p-1'>{file.name}</li>
      ))}
    </ul>
  )
}