import axios from 'axios'
import type { LoginResponse, Order, CreateOrderRequest, PagedResult, Tracking } from '../types'

// Vite proxy handles /api/* → correct backend
// So no need to hardcode ports here
const orderApi   = axios.create({ baseURL: '/api' })
const trackApi   = axios.create({ baseURL: '/api' })

// Auto-attach JWT token to every request
const authInterceptor = (config: any) => {
  const token = localStorage.getItem('token')
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
}
orderApi.interceptors.request.use(authInterceptor)
trackApi.interceptors.request.use(authInterceptor)

// Redirect to login on 401
orderApi.interceptors.response.use(
  res => res,
  err => {
    if (err.response?.status === 401) {
      localStorage.removeItem('token')
      window.location.href = '/login'
    }
    return Promise.reject(err)
  }
)

// ── Auth ──────────────────────────────────────────────────
export const login = (username: string, password: string) =>
  orderApi.post<LoginResponse>('/auth/login', { username, password })
    .then(r => r.data)

// ── Orders ────────────────────────────────────────────────
export const getOrders = (page = 1, pageSize = 10) =>
  orderApi.get<PagedResult<Order>>(`/orders?page=${page}&pageSize=${pageSize}`)
    .then(r => r.data)

export const getOrder = (id: string) =>
  orderApi.get<Order>(`/orders/${id}`).then(r => r.data)

export const createOrder = (body: CreateOrderRequest) =>
  orderApi.post<Order>('/orders', body).then(r => r.data)

export const updateStatus = (id: string, status: string) =>
  orderApi.patch<Order>(`/orders/${id}/status`, { status }).then(r => r.data)

export const uploadFile = (orderId: string, file: File) => {
  const fd = new FormData()
  fd.append('file', file)
  return orderApi.post(`/orders/${orderId}/attachments`, fd).then(r => r.data)
}

// ── Tracking ──────────────────────────────────────────────
export const getTracking = (orderCode: string) =>
  trackApi.get<Tracking>(`/tracking/${orderCode}`).then(r => r.data)
