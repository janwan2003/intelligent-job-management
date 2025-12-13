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
        <h1>Embedding Model Selection Platform</h1>
        <p>Find the best embedding model for your use case</p>
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
