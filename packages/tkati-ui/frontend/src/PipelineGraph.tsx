import { useMemo } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  type Node,
  type Edge,
  BackgroundVariant,
  MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import dagre from '@dagrejs/dagre'
import type { Manifest } from './types'
import { TopicNode } from './nodes/TopicNode'
import { ProcessorNode } from './nodes/ProcessorNode'

const nodeTypes = {
  topic: TopicNode,
  processor: ProcessorNode,
}

const TOPIC_W = 200
const TOPIC_H = 66
const PROC_W = 220
const PROC_H = 88

function applyDagreLayout(nodes: Node[], edges: Edge[]): Node[] {
  const g = new dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'LR', ranksep: 120, nodesep: 40, marginx: 40, marginy: 40 })

  for (const n of nodes) {
    const w = n.type === 'processor' ? PROC_W : TOPIC_W
    const h = n.type === 'processor' ? PROC_H : TOPIC_H
    g.setNode(n.id, { width: w, height: h })
  }
  for (const e of edges) g.setEdge(e.source, e.target)

  dagre.layout(g)

  return nodes.map(n => {
    const { x, y } = g.node(n.id)
    const w = n.type === 'processor' ? PROC_W : TOPIC_W
    const h = n.type === 'processor' ? PROC_H : TOPIC_H
    return { ...n, position: { x: x - w / 2, y: y - h / 2 } }
  })
}

export default function PipelineGraph({ manifest }: { manifest: Manifest }) {
  const { nodes, edges } = useMemo(() => {
    const rawNodes: Node[] = []
    const rawEdges: Edge[] = []

    for (const [key, topic] of Object.entries(manifest.kafka_topics)) {
      rawNodes.push({
        id: `topic:${key}`,
        type: 'topic',
        data: { topic, topicKey: key },
        position: { x: 0, y: 0 },
      })
    }

    for (const [key, node] of Object.entries(manifest.nodes)) {
      rawNodes.push({
        id: `node:${key}`,
        type: 'processor',
        data: { node },
        position: { x: 0, y: 0 },
      })
      for (const input of node.inputs) {
        rawEdges.push({
          id: `${input.topic}→${key}`,
          source: `topic:${input.topic}`,
          target: `node:${key}`,
          markerEnd: { type: MarkerType.ArrowClosed, color: '#475569' },
          style: { stroke: '#334155', strokeWidth: 1.5 },
        })
      }
      for (const output of node.outputs) {
        rawEdges.push({
          id: `${key}→${output.topic}`,
          source: `node:${key}`,
          target: `topic:${output.topic}`,
          markerEnd: { type: MarkerType.ArrowClosed, color: '#475569' },
          style: { stroke: '#334155', strokeWidth: 1.5 },
        })
      }
    }

    return { nodes: applyDagreLayout(rawNodes, rawEdges), edges: rawEdges }
  }, [manifest])

  return (
    <div style={{ width: '100%', height: '100%' }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.2}
        proOptions={{ hideAttribution: true }}
      >
        <Background color="#1e293b" variant={BackgroundVariant.Dots} gap={24} size={1.5} />
        <Controls style={{ background: '#1e293b', border: '1px solid #334155' }} />
        <MiniMap
          style={{ background: '#0f172a', border: '1px solid #1e293b' }}
          nodeColor={n => n.type === 'processor' ? '#3730a3' : '#334155'}
        />
      </ReactFlow>
    </div>
  )
}
