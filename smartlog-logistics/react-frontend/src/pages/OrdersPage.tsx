import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { getOrders, updateStatus } from '../services/api'
import type { Order, PagedResult } from '../types'

const STATUS_COLOR: Record<string, string> = {
  Pending:    '#faad14',
  Confirmed:  '#1677ff',
  PickedUp:   '#722ed1',
  InTransit:  '#13c2c2',
  Delivered:  '#52c41a',
  Cancelled:  '#f5222d',
}

export default function OrdersPage() {
  const [result, setResult]   = useState<PagedResult<Order> | null>(null)
  const [page, setPage]       = useState(1)
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  const load = async (p: number) => {
    setLoading(true)
    try { setResult(await getOrders(p)) }
    finally { setLoading(false) }
  }

  useEffect(() => { load(page) }, [page])

  const handleStatus = async (id: string, status: string) => {
    const next = prompt(`New status:\nPending/Confirmed/PickedUp/InTransit/Delivered/Cancelled`, status)
    if (!next) return
    try {
      await updateStatus(id, next)
      load(page)
    } catch { alert('Invalid status') }
  }

  return (
    <div style={styles.page}>
      <div style={styles.header}>
        <h2 style={{ margin:0 }}>📦 Orders</h2>
        <div style={{ display:'flex', gap:8 }}>
          <button style={styles.btnPrimary} onClick={() => navigate('/orders/create')}>
            + Tạo đơn hàng
          </button>
          <button style={styles.btnSecondary} onClick={() => navigate('/tracking')}>
            🗺️ Tracking
          </button>
          <button style={styles.btnSecondary} onClick={() => { localStorage.removeItem('token'); navigate('/login') }}>
            Logout
          </button>
        </div>
      </div>

      {loading && <p>Đang tải...</p>}

      {result && (
        <>
          <p style={styles.total}>Tổng: {result.total} đơn hàng</p>
          <div style={styles.tableWrap}>
            <table style={styles.table}>
              <thead>
                <tr style={styles.thead}>
                  {['Mã đơn','Khách hàng','Điểm giao','Tổng kg','Phí ship','Trạng thái','Ngày tạo',''].map(h => (
                    <th key={h} style={styles.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {result.data.map(o => (
                  <tr key={o.id} style={styles.tr}>
                    <td style={styles.td}>
                      <strong style={{ color:'#1677ff', cursor:'pointer' }}
                        onClick={() => navigate(`/orders/${o.id}`)}>
                        {o.orderCode}
                      </strong>
                    </td>
                    <td style={styles.td}>{o.customerName}</td>
                    <td style={{ ...styles.td, maxWidth:180, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                      {o.destinationAddress}
                    </td>
                    <td style={styles.td}>{o.totalWeight} kg</td>
                    <td style={styles.td}>{o.shippingFee.toLocaleString('vi-VN')}đ</td>
                    <td style={styles.td}>
                      <span style={{ ...styles.badge, background: STATUS_COLOR[o.status] + '20', color: STATUS_COLOR[o.status] }}>
                        {o.status}
                      </span>
                    </td>
                    <td style={styles.td}>{new Date(o.createdAt).toLocaleDateString('vi-VN')}</td>
                    <td style={styles.td}>
                      <button style={styles.btnXs} onClick={() => handleStatus(o.id, o.status)}>
                        Cập nhật
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div style={styles.pagination}>
            <button disabled={page <= 1} onClick={() => setPage(p => p - 1)} style={styles.btnSecondary}>← Trước</button>
            <span>Trang {page} / {result.totalPages}</span>
            <button disabled={page >= result.totalPages} onClick={() => setPage(p => p + 1)} style={styles.btnSecondary}>Sau →</button>
          </div>
        </>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  page:       { padding:24, maxWidth:1200, margin:'0 auto' },
  header:     { display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:20 },
  total:      { color:'#666', marginBottom:12 },
  tableWrap:  { overflowX:'auto' },
  table:      { width:'100%', borderCollapse:'collapse', background:'#fff', borderRadius:8, overflow:'hidden', boxShadow:'0 2px 8px rgba(0,0,0,0.06)' },
  thead:      { background:'#fafafa' },
  th:         { padding:'12px 16px', textAlign:'left', fontWeight:600, fontSize:13, color:'#555', borderBottom:'1px solid #f0f0f0' },
  tr:         { borderBottom:'1px solid #f5f5f5' },
  td:         { padding:'12px 16px', fontSize:13 },
  badge:      { padding:'2px 10px', borderRadius:12, fontSize:12, fontWeight:500 },
  btnPrimary: { padding:'8px 16px', background:'#1677ff', color:'#fff', border:'none', borderRadius:6, cursor:'pointer', fontSize:13 },
  btnSecondary:{ padding:'8px 16px', background:'#fff', border:'1px solid #ddd', borderRadius:6, cursor:'pointer', fontSize:13 },
  btnXs:      { padding:'4px 10px', background:'#fff', border:'1px solid #ddd', borderRadius:4, cursor:'pointer', fontSize:12 },
  pagination: { display:'flex', gap:12, alignItems:'center', justifyContent:'center', marginTop:20 },
}
