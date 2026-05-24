import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import LoginPage       from './pages/LoginPage'
import OrdersPage      from './pages/OrdersPage'
import CreateOrderPage from './pages/CreateOrderPage'
import TrackingPage    from './pages/TrackingPage'

// Simple auth guard
function Private({ children }: { children: React.ReactNode }) {
  return localStorage.getItem('token')
    ? <>{children}</>
    : <Navigate to="/login" replace />
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login"   element={<LoginPage />} />
        <Route path="/tracking" element={<TrackingPage />} />
        <Route path="/orders"  element={<Private><OrdersPage /></Private>} />
        <Route path="/orders/create" element={<Private><CreateOrderPage /></Private>} />
        <Route path="*" element={<Navigate to="/orders" replace />} />
      </Routes>
    </BrowserRouter>
  )
}
