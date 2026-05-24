import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { getTracking } from '../services/api'
import type { Tracking } from '../types'

const STATUS_COLOR: Record<string, string> = {
  Pending:'#faad14', Confirmed:'#1677ff', PickedUp:'#722ed1',
  InTransit:'#13c2c2', Delivered:'#52c41a', Cancelled:'#f5222d',
}

export default function TrackingPage() {
  const [code, setCode]         = useState('')
  const [data, setData]         = useState<Tracking | null>(null)
  const [loading, setLoading]   = useState(false)
  const [notFound, setNotFound] = useState(false)
  const navigate = useNavigate()

  const search = async () => {
    if (!code.trim()) return
    setLoading(true); setNotFound(false); setData(null)
    try { setData(await getTracking(code.trim())) }
    catch { setNotFound(true) }
    finally { setLoading(false) }
  }

  return (
    <div style={styles.page}>
      <div style={styles.card}>
        <div style={styles.header}>
          <h2 style={{ margin:0 }}>🗺️ Theo dõi đơn hàng</h2>
          <button style={styles.btnBack} onClick={() => navigate('/orders')}>← Orders</button>
        </div>

        <div style={styles.searchRow}>
          <input
            style={styles.input}
            placeholder="Nhập mã đơn hàng (VD: SML-20240101-ABC123)"
            value={code}
            onChange={e => setCode(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && search()}
          />
          <button style={styles.btnSearch} onClick={search} disabled={loading}>
            {loading ? '...' : '🔍 Tra cứu'}
          </button>
        </div>

        {notFound && (
          <div style={styles.notFound}>
            ❌ Không tìm thấy đơn hàng <strong>{code}</strong>
          </div>
        )}

        {data && (
          <div style={styles.result}>
            {/* Summary */}
            <div style={styles.summary}>
              <div>
                <div style={styles.orderCode}>{data.orderCode}</div>
                <div style={styles.customer}>👤 {data.customerName}</div>
              </div>
              <span style={{
                ...styles.statusBadge,
                background: (STATUS_COLOR[data.currentStatus] ?? '#999') + '20',
                color: STATUS_COLOR[data.currentStatus] ?? '#999'
              }}>
                {data.currentStatus}
              </span>
            </div>

            {/* Timeline */}
            <h4 style={{ marginBottom:16, color:'#444' }}>Lịch trình</h4>
            <div style={styles.timeline}>
              {data.timeline.map((e, i) => (
                <div key={i} style={styles.event}>
                  <div style={styles.dotCol}>
                    <div style={{
                      ...styles.dot,
                      background: i === 0 ? (STATUS_COLOR[e.status] ?? '#1677ff') : '#d9d9d9'
                    }} />
                    {i < data.timeline.length - 1 && <div style={styles.line} />}
                  </div>
                  <div style={styles.eventInfo}>
                    <div style={styles.eventStatus}>
                      <strong>{e.status}</strong>
                      <span style={styles.location}>📍 {e.location}</span>
                    </div>
                    <p style={styles.note}>{e.note}</p>
                    <small style={styles.time}>
                      {new Date(e.occurredAt).toLocaleString('vi-VN')}
                    </small>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  page:        { padding:24, maxWidth:720, margin:'0 auto' },
  card:        { background:'#fff', borderRadius:12, padding:32, boxShadow:'0 2px 12px rgba(0,0,0,0.08)' },
  header:      { display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:24 },
  searchRow:   { display:'flex', gap:10, marginBottom:24 },
  input:       { flex:1, padding:'11px 14px', border:'1px solid #ddd', borderRadius:8, fontSize:14 },
  btnSearch:   { padding:'11px 20px', background:'#1677ff', color:'#fff', border:'none', borderRadius:8, cursor:'pointer', fontSize:14, whiteSpace:'nowrap' },
  btnBack:     { padding:'8px 16px', background:'#fff', border:'1px solid #ddd', borderRadius:6, cursor:'pointer', fontSize:13 },
  notFound:    { padding:16, background:'#fff2f0', border:'1px solid #ffccc7', borderRadius:8, color:'#f5222d' },
  result:      { marginTop:8 },
  summary:     { display:'flex', justifyContent:'space-between', alignItems:'center', padding:20, background:'#f8faff', borderRadius:8, marginBottom:24 },
  orderCode:   { fontSize:20, fontWeight:700, color:'#1677ff' },
  customer:    { marginTop:6, color:'#555', fontSize:14 },
  statusBadge: { padding:'6px 16px', borderRadius:16, fontWeight:600, fontSize:14 },
  timeline:    { paddingLeft:8 },
  event:       { display:'flex', gap:16, marginBottom:4 },
  dotCol:      { display:'flex', flexDirection:'column', alignItems:'center', minWidth:16 },
  dot:         { width:14, height:14, borderRadius:'50%', flexShrink:0, marginTop:3 },
  line:        { width:2, flex:1, background:'#f0f0f0', minHeight:32, margin:'4px 0' },
  eventInfo:   { flex:1, paddingBottom:20 },
  eventStatus: { display:'flex', gap:12, alignItems:'center', marginBottom:4 },
  location:    { color:'#888', fontSize:13 },
  note:        { margin:'4px 0', color:'#555', fontSize:13 },
  time:        { color:'#aaa', fontSize:12 },
}
