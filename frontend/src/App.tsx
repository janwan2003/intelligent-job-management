import { useEffect, useState } from 'react'
import './App.css'

interface Job {
  id: string
  image: string
  command: string[]
  status: string
  created_at: string
  updated_at: string
  container_name?: string
  exit_code?: number
}

const API_BASE = 'http://localhost:8000'

function App() {
  const [jobs, setJobs] = useState<Job[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Form state
  const [image, setImage] = useState('ijm-runtime:dev')
  const [command, setCommand] = useState('python -u train.py')
  const [submitting, setSubmitting] = useState(false)
  const [uploading, setUploading] = useState(false)
  const [uploadMessage, setUploadMessage] = useState<string | null>(null)

  // Fetch jobs from API
  const fetchJobs = async () => {
    try {
      const res = await fetch(`${API_BASE}/jobs`)
      if (!res.ok) throw new Error('Failed to fetch jobs')
      const data = await res.json()
      setJobs(data)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  // Poll for jobs every 3 seconds
  useEffect(() => {
    fetchJobs()
    const interval = setInterval(fetchJobs, 3000)
    return () => clearInterval(interval)
  }, [])

  // Submit new job
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSubmitting(true)

    try {
      const commandArray = command.split(' ').filter((s) => s.trim() !== '')
      const res = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ image, command: commandArray }),
      })

      if (!res.ok) throw new Error('Failed to submit job')

      // Refresh jobs list
      await fetchJobs()
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to submit job')
    } finally {
      setSubmitting(false)
    }
  }

  // Stop a job
  const handleStop = async (jobId: string) => {
    try {
      const res = await fetch(`${API_BASE}/jobs/${jobId}/stop`, {
        method: 'POST',
      })
      if (!res.ok) throw new Error('Failed to stop job')
      await fetchJobs()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to stop job')
    }
  }

  // Resume a job
  const handleResume = async (jobId: string) => {
    try {
      const res = await fetch(`${API_BASE}/jobs/${jobId}/resume`, {
        method: 'POST',
      })
      if (!res.ok) throw new Error('Failed to resume job')
      await fetchJobs()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to resume job')
    }
  }

  // Delete a job
  const handleDelete = async (jobId: string) => {
    if (!confirm('Are you sure you want to delete this job?')) return

    try {
      const res = await fetch(`${API_BASE}/jobs/${jobId}`, {
        method: 'DELETE',
      })
      if (!res.ok) throw new Error('Failed to delete job')
      await fetchJobs()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete job')
    }
  }

  // Upload Docker image file
  const handleImageUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return

    setUploading(true)
    setUploadMessage(null)
    setError(null)

    try {
      const formData = new FormData()
      formData.append('file', file)

      const res = await fetch(`${API_BASE}/images/upload`, {
        method: 'POST',
        body: formData,
      })

      if (!res.ok) {
        const errorData = await res.json()
        throw new Error(errorData.detail || 'Failed to upload image')
      }

      const data = await res.json()
      setImage(data.image)
      setUploadMessage(`✓ ${data.message}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to upload image')
    } finally {
      setUploading(false)
    }
  }

  // Get status badge color
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'QUEUED':
        return '#888'
      case 'RUNNING':
        return '#4a90e2'
      case 'SUCCEEDED':
        return '#50c878'
      case 'FAILED':
        return '#e74c3c'
      case 'PREEMPTED':
        return '#f39c12'
      default:
        return '#888'
    }
  }

  return (
    <>
      <div>
        <h1>Intelligent Job Management System</h1>
        <p>Submit and manage GPU training jobs</p>
      </div>

      {error && (
        <div style={{ color: 'red', padding: '1rem', border: '1px solid red', borderRadius: '4px', marginBottom: '1rem' }}>
          Error: {error}
        </div>
      )}

      {uploadMessage && (
        <div style={{ color: '#50c878', padding: '1rem', border: '1px solid #50c878', borderRadius: '4px', marginBottom: '1rem' }}>
          {uploadMessage}
        </div>
      )}

      <div className="card">
        <h2>Submit New Job</h2>
        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
          <div>
            <label htmlFor="image" style={{ display: 'block', marginBottom: '0.5rem' }}>
              Docker Image:
            </label>
            <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
              <input
                id="image"
                type="text"
                value={image}
                onChange={(e) => setImage(e.target.value)}
                required
                style={{ flex: 1, padding: '0.5rem', fontSize: '1rem' }}
              />
              <label
                htmlFor="image-upload"
                style={{
                  padding: '0.5rem 1rem',
                  backgroundColor: '#555',
                  border: 'none',
                  borderRadius: '4px',
                  cursor: uploading ? 'not-allowed' : 'pointer',
                  opacity: uploading ? 0.6 : 1,
                  whiteSpace: 'nowrap',
                }}
              >
                {uploading ? 'Uploading...' : '📁 Upload .tar'}
              </label>
              <input
                id="image-upload"
                type="file"
                accept=".tar,.tar.gz,.tgz"
                onChange={handleImageUpload}
                disabled={uploading}
                style={{ display: 'none' }}
              />
            </div>
            <small style={{ color: '#888', fontSize: '0.875rem' }}>
              Enter image name or upload a .tar/.tar.gz file
            </small>
          </div>
          <div>
            <label htmlFor="command" style={{ display: 'block', marginBottom: '0.5rem' }}>
              Command (space-separated):
            </label>
            <input
              id="command"
              type="text"
              value={command}
              onChange={(e) => setCommand(e.target.value)}
              required
              style={{ width: '100%', padding: '0.5rem', fontSize: '1rem' }}
            />
          </div>
          <button type="submit" disabled={submitting} style={{ padding: '0.75rem', fontSize: '1rem' }}>
            {submitting ? 'Submitting...' : 'Submit Job'}
          </button>
        </form>
      </div>

      <div className="card">
        <h2>Jobs</h2>
        {loading ? (
          <p>Loading jobs...</p>
        ) : jobs.length === 0 ? (
          <p>No jobs yet. Submit one above!</p>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
            {jobs.map((job) => (
              <div
                key={job.id}
                style={{
                  border: '1px solid #444',
                  borderRadius: '8px',
                  padding: '1rem',
                  backgroundColor: '#1a1a1a',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                  <div>
                    <strong>Job ID:</strong> {job.id.slice(0, 8)}...
                  </div>
                  <div
                    style={{
                      padding: '0.25rem 0.75rem',
                      borderRadius: '12px',
                      backgroundColor: getStatusColor(job.status),
                      color: 'white',
                      fontSize: '0.875rem',
                      fontWeight: 'bold',
                    }}
                  >
                    {job.status}
                  </div>
                </div>
                <div style={{ marginBottom: '0.5rem' }}>
                  <strong>Image:</strong> {job.image}
                </div>
                <div style={{ marginBottom: '0.5rem' }}>
                  <strong>Command:</strong> {job.command.join(' ')}
                </div>
                <div style={{ fontSize: '0.875rem', color: '#888', marginBottom: '0.75rem' }}>
                  Created: {new Date(job.created_at).toLocaleString()}
                  {job.exit_code !== null && job.exit_code !== undefined && (
                    <> | Exit Code: {job.exit_code}</>
                  )}
                </div>
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  {(job.status === 'RUNNING' || job.status === 'QUEUED') && (
                    <button
                      onClick={() => handleStop(job.id)}
                      style={{ padding: '0.5rem 1rem', backgroundColor: '#e74c3c', border: 'none', borderRadius: '4px', cursor: 'pointer', color: 'white' }}
                    >
                      Stop
                    </button>
                  )}
                  {(job.status === 'PREEMPTED' || job.status === 'FAILED') && (
                    <button
                      onClick={() => handleResume(job.id)}
                      style={{ padding: '0.5rem 1rem', backgroundColor: '#4a90e2', border: 'none', borderRadius: '4px', cursor: 'pointer', color: 'white' }}
                    >
                      Resume
                    </button>
                  )}
                  <button
                    onClick={() => handleDelete(job.id)}
                    style={{ padding: '0.5rem 1rem', backgroundColor: '#555', border: 'none', borderRadius: '4px', cursor: 'pointer', color: 'white' }}
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </>
  )
}

export default App

