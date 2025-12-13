import { useEffect, useState } from 'react'
import './App.css'

function App() {
  const [status, setStatus] = useState<{ status: string; message: string } | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('http://localhost:8000/')
      .then((res) => res.json())
      .then((data) => {
        setStatus(data)
        setLoading(false)
      })
      .catch((err) => {
        console.error('Failed to fetch API:', err)
        setLoading(false)
      })
  }, [])

  return (
    <>
      <div>
        <h1>Intelligent Job Management System for GPU-Accelerated Deep Learning Clusters</h1>
        <p>Queue models and have fun!</p>
      </div>
      <div className="card">
        <h2>Backend API Status</h2>
        {loading ? (
          <p>Loading...</p>
        ) : status ? (
          <div>
            <p>Status: <strong>{status.status}</strong></p>
            <p>{status.message}</p>
          </div>
        ) : (
          <p style={{ color: 'red' }}>Failed to connect to backend</p>
        )}
      </div>
    </>
  )
}

export default App
