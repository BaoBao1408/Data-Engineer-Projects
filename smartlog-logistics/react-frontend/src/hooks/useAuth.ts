import { useState } from 'react'
import { login as apiLogin } from '../services/api'

export function useAuth() {
  const [token, setToken] = useState<string | null>(
    localStorage.getItem('token')
  )

  const login = async (username: string, password: string) => {
    const res = await apiLogin(username, password)
    localStorage.setItem('token', res.token)
    setToken(res.token)
  }

  const logout = () => {
    localStorage.removeItem('token')
    setToken(null)
  }

  return { token, isLoggedIn: !!token, login, logout }
}
