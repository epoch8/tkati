import { useEffect, useState } from 'react'
import PipelineGraph from './PipelineGraph'
import type { Manifest } from './types'

const LABEL: React.CSSProperties = {
  fontFamily: 'ui-monospace, monospace',
  fontSize: 13,
  color: '#64748b',
  padding: '0 20px',
  display: 'flex',
  alignItems: 'center',
  height: '100%',
}

export default function App() {
  const [manifest, setManifest] = useState<Manifest | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch('/api/manifest')
      .then(r => r.json())
      .then(data => {
        if (data.detail) setError(String(data.detail))
        else setManifest(data as Manifest)
      })
      .catch(e => setError(String(e)))
  }, [])

  if (error) return <div style={{ ...LABEL, color: '#f87171', whiteSpace: 'pre-wrap' }}>{error}</div>
  if (!manifest) return <div style={LABEL}>Loading…</div>

  return <PipelineGraph manifest={manifest} />
}
