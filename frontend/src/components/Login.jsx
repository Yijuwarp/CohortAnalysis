import { useState } from 'react'
import { login } from '../api'

export default function Login({ onLogin }) {
  const [email, setEmail] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const handleSubmit = async (e) => {
    e.preventDefault()
    if (!email) return

    setLoading(true)
    setError('')
    try {
      const data = await login(email)
      
      // Strict validation of the returned user_id
      if (!data.user_id || !/^[a-f0-9]{8}$/.test(data.user_id)) {
        throw new Error('Server returned an invalid session ID')
      }
      
      localStorage.setItem('user_id', data.user_id)
      onLogin(data.user_id)
    } catch (err) {
      setError(err.message || 'Login failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="login-container" style={{
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      height: '100vh',
      backgroundColor: '#f9fafb'
    }}>
      <div className="card ui-card" style={{
        width: '100%',
        maxWidth: '400px',
        padding: 'var(--space-6)',
        textAlign: 'center',
        boxShadow: 'var(--shadow-sm)',
        borderRadius: 'var(--radius-md)',
        backgroundColor: 'var(--color-bg)',
        border: '1px solid var(--color-border)'
      }}>
        <h2 style={{
          fontSize: 'var(--font-size-xl)',
          color: 'var(--color-text-primary)',
          marginBottom: 'var(--space-2)'
        }}>Login</h2>
        <p style={{
          color: 'var(--color-text-secondary)',
          fontSize: 'var(--font-size-sm)',
          marginBottom: 'var(--space-5)'
        }}>
          Enter your email to continue to the workspace.
        </p>
        
        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: 'var(--space-4)' }}>
          <input
            type="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            placeholder="Enter email"
            required
            className="input"
            style={{
              padding: 'var(--space-3) var(--space-4)',
              borderRadius: 'var(--radius-sm)',
              border: '1px solid var(--color-border)',
              fontSize: 'var(--font-size-md)',
              outline: 'none'
            }}
          />
          
          {error && (
            <div style={{
              color: '#dc2626',
              fontSize: 'var(--font-size-xs)',
              textAlign: 'left'
            }}>
              {error}
            </div>
          )}

          <button
            type="submit"
            className="button"
            disabled={loading || !email}
            style={{
              padding: 'var(--space-3)',
              backgroundColor: 'var(--color-primary)',
              color: 'white',
              border: 'none',
              borderRadius: 'var(--radius-sm)',
              fontSize: 'var(--font-size-md)',
              fontWeight: '600',
              cursor: 'pointer',
              transition: 'background-color 0.2s'
            }}
          >
            {loading ? 'Logging in...' : 'Continue'}
          </button>
        </form>
      </div>
    </div>
  )
}
