import { type XYCoord, useDragLayer } from 'react-dnd'
import { useMemo, type CSSProperties } from 'react'
import Directory from './Directory'
import File from './File'
import { ModelDirectory } from '@models/directory'
import { useFileExplorer } from './context'
import FileExplorer from './FileExplorer'
import { ModelFile } from '@models/file'

const layerStyles: CSSProperties = {
  position: 'fixed',
  pointerEvents: 'none',
  zIndex: 100,
  left: 0,
  top: 0,
  width: '100%',
  height: '100%',
}

export default function DragLayer(): JSX.Element {
  const { activeRange } = useFileExplorer()

  const { isDragging, currentOffset, artifact } = useDragLayer(monitor => ({
    artifact: monitor.getItem(),
    isDragging: monitor.isDragging(),
    currentOffset: monitor.getSourceClientOffset(),
  }))
  const artifacts = useMemo(
    () =>
      Array.from(
        activeRange.has(artifact) ? activeRange : new Set([artifact]),
      ).filter(Boolean),
    [activeRange, artifact],
  )

  return (
    <>
      {isDragging && (
        <div
          style={layerStyles}
          className="!cursor-grabbing"
        >
          <div
            style={getItemStyles(currentOffset)}
            className="!cursor-grabbing"
          >
            {artifacts.map(artifact => (
              <span key={artifact.id}>
                {artifact instanceof ModelDirectory && (
                  <FileExplorer.Container
                    key={artifact.path}
                    artifact={artifact}
                    className="bg-theme"
                  >
                    <Directory.Icons hasChevron={false} />
                    <Directory.Display directory={artifact} />
                  </FileExplorer.Container>
                )}
                {artifact instanceof ModelFile && (
                  <FileExplorer.Container
                    key={artifact.path}
                    artifact={artifact}
                    className="bg-theme"
                  >
                    <File.Icons />
                    <File.Display file={artifact} />
                  </FileExplorer.Container>
                )}
              </span>
            ))}
          </div>
        </div>
      )}
    </>
  )
}

function getItemStyles(currentOffset: XYCoord | null): React.CSSProperties {
  if (currentOffset == null) {
    return {
      display: 'none',
    }
  }

  const { x, y } = currentOffset

  return {
    display: 'inline-block',
    transform: `translate(${x}px, ${y}px)`,
    filter: 'drop-shadow(0 2px 12px rgba(0,0,0,0.45))',
  }
}
