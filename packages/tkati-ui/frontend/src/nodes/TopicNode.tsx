import { Handle, Position, type NodeProps } from '@xyflow/react'
import type { KafkaTopic } from '../types'

export interface TopicNodeData {
  topic: KafkaTopic
  topicKey: string
  [key: string]: unknown
}

export function TopicNode({ data, selected }: NodeProps) {
  const { topic } = data as TopicNodeData
  return (
    <div style={{
      background: '#1e293b',
      border: `1.5px solid ${selected ? '#60a5fa' : '#334155'}`,
      borderRadius: 8,
      padding: '10px 14px',
      minWidth: 190,
      fontFamily: 'ui-monospace, monospace',
      boxShadow: selected ? '0 0 0 2px #3b82f620' : 'none',
    }}>
      <Handle type="target" position={Position.Left} style={{ background: '#475569' }} />

      <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
        <span style={{ fontSize: 13, color: '#94a3b8' }}>⬡</span>
        <span style={{ fontSize: 13, fontWeight: 600, color: '#e2e8f0' }}>{topic.name}</span>
      </div>

      <div style={{ fontSize: 11, color: '#64748b', lineHeight: 1.6 }}>
        <span style={{ color: '#475569' }}>{topic.cluster}</span>
        {' · '}
        <span>{topic.schema}</span>
        {' · '}
        <span>{topic.partitions}p</span>
      </div>

      <Handle type="source" position={Position.Right} style={{ background: '#475569' }} />
    </div>
  )
}
