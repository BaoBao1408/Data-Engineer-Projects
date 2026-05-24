import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../hooks/useAuth'

export default function LoginPage() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError]       = useState('')
  const [loading, setLoading]   = useState(false)
  const { login } = useAuth()
  const navigate  = useNavigate()

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true); setError('')
    try {
      await login(username, password)
      navigate('/orders')
    } catch {
      setError('Sai tài khoản hoặc mật khẩu')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={styles.container}>
      <div style={styles.card}>
        <h2 style={styles.title}>🚀 Smartlog</h2>
        <form onSubmit={handleSubmit}>
          <input
            style={styles.input}
            placeholder="Username"
            value={username}
            onChange={e => setUsername(e.target.value)}
          />
          <input
            style={styles.input}
            type="password"
            placeholder="Password"
            value={password}
            onChange={e => setPassword(e.target.value)}
          />
          {error && <p style={styles.error}>{error}</p>}
          <button style={styles.btn} type="submit" disabled={loading}>
            {loading ? 'Đang đăng nhập...' : 'Đăng nhập'}
          </button>
        </form>
        <p style={styles.hint}>Demo: admin / smartlog123</p>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: { display:'flex', justifyContent:'center', alignItems:'center', minHeight:'100vh', background:'#f0f2f5' },
  card:      { background:'#fff', padding:40, borderRadius:12, boxShadow:'0 4px 20px rgba(0,0,0,0.1)', minWidth:360 },
  title:     { textAlign:'center', marginBottom:24, fontSize:24 },
  input:     { width:'100%', padding:'10px 14px', marginBottom:12, border:'1px solid #ddd', borderRadius:6, fontSize:14, boxSizing:'border-box' },
  btn:       { width:'100%', padding:'12px', background:'#1677ff', color:'#fff', border:'none', borderRadius:6, fontSize:15, cursor:'pointer' },
  error:     { color:'#f5222d', fontSize:13, marginBottom:8 },
  hint:      { textAlign:'center', color:'#999', fontSize:12, marginTop:12 },
}
