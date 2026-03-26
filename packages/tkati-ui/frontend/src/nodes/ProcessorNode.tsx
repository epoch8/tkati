import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { PipelineNode } from '../types'

export interface ProcessorNodeData {
  node: PipelineNode
  [key: string]: unknown
}

export function ProcessorNode({ data, selected }: NodeProps) {
  const { node } = data as ProcessorNodeData
  const [pkg, fn] = node.handler.includes('.')
    ? [node.handler.slice(0, node.handler.lastIndexOf('.')), node.handler.slice(node.handler.lastIndexOf('.') + 1)]
    : ['', node.handler]
  return (
    <div style={{
      background: '#1e1b4b',
      border: `1.5px solid ${selected ? '#a78bfa' : '#3730a3'}`,
      borderRadius: 8,
      padding: '10px 14px',
      minWidth: 210,
      fontFamily: 'ui-monospace, monospace',
      boxShadow: selected ? '0 0 0 2px #7c3aed20' : 'none',
    }}>
      <Handle type="target" position={Position.Left} style={{ background: '#6d28d9' }} />

      <div style={{ fontSize: 13, fontWeight: 700, color: '#e2e8f0', marginBottom: 6 }}>
        {node.name}
      </div>

      <div style={{ fontSize: 11, color: '#818cf8', lineHeight: 1.5 }}>
        {pkg && <span style={{ color: '#4f46e5' }}>{pkg}.</span>}
        <span>{fn}</span>
      </div>

      {node.deploy?.image && (
        <div style={{ fontSize: 10, color: '#4338ca', marginTop: 4, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {node.deploy.image}
        </div>
      )}

      <Handle type="source" position={Position.Right} style={{ background: '#6d28d9' }} />
    </div>
  )
}
