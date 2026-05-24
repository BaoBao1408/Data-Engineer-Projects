import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { createOrder } from '../services/api'

interface Item { productName: string; quantity: number; weight: number }

export default function CreateOrderPage() {
  const navigate = useNavigate()
  const [loading, setLoading] = useState(false)
  const [form, setForm] = useState({
    customerName: '', customerPhone: '',
    originAddress: '', destinationAddress: ''
  })
  const [items, setItems] = useState<Item[]>([
    { productName: '', quantity: 1, weight: 0 }
  ])

  const setField = (k: keyof typeof form, v: string) =>
    setForm(f => ({ ...f, [k]: v }))

  const setItem = (i: number, k: keyof Item, v: string | number) =>
    setItems(prev => prev.map((item, idx) =>
      idx === i ? { ...item, [k]: v } : item
    ))

  const addItem    = () => setItems(p => [...p, { productName:'', quantity:1, weight:0 }])
  const removeItem = (i: number) => setItems(p => p.filter((_, idx) => idx !== i))

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    try {
      const order = await createOrder({ ...form, items })
      alert(`✅ Tạo đơn thành công: ${order.orderCode}`)
      navigate('/orders')
    } catch (err: any) {
      alert('Lỗi: ' + (err.response?.data?.message ?? err.message))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={styles.page}>
      <div style={styles.card}>
        <div style={styles.header}>
          <h2 style={{ margin:0 }}>📦 Tạo đơn hàng mới</h2>
          <button style={styles.btnBack} onClick={() => navigate('/orders')}>← Quay lại</button>
        </div>

        <form onSubmit={handleSubmit}>
          {/* Customer info */}
          <h4 style={styles.section}>Thông tin khách hàng</h4>
          <div style={styles.row}>
            <div style={styles.col}>
              <label style={styles.label}>Tên khách hàng *</label>
              <input style={styles.input} required
                value={form.customerName}
                onChange={e => setField('customerName', e.target.value)}
                placeholder="Nguyễn Văn A" />
            </div>
            <div style={styles.col}>
              <label style={styles.label}>Số điện thoại *</label>
              <input style={styles.input} required
                value={form.customerPhone}
                onChange={e => setField('customerPhone', e.target.value)}
                placeholder="0901234567" />
            </div>
          </div>

          {/* Addresses */}
          <h4 style={styles.section}>Địa chỉ</h4>
          <div style={styles.row}>
            <div style={styles.col}>
              <label style={styles.label}>Địa chỉ lấy hàng *</label>
              <input style={styles.input} required
                value={form.originAddress}
                onChange={e => setField('originAddress', e.target.value)}
                placeholder="123 Nguyễn Huệ, Q1, HCM" />
            </div>
            <div style={styles.col}>
              <label style={styles.label}>Địa chỉ giao hàng *</label>
              <input style={styles.input} required
                value={form.destinationAddress}
                onChange={e => setField('destinationAddress', e.target.value)}
                placeholder="456 Lê Lợi, Q3, HCM" />
            </div>
          </div>

          {/* Items */}
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginTop:24 }}>
            <h4 style={{ margin:0 }}>📋 Danh sách hàng hóa</h4>
            <button type="button" style={styles.btnAdd} onClick={addItem}>+ Thêm hàng</button>
          </div>

          {items.map((item, i) => (
            <div key={i} style={styles.itemRow}>
              <input style={{ ...styles.input, flex:2 }} required
                placeholder="Tên sản phẩm"
                value={item.productName}
                onChange={e => setItem(i, 'productName', e.target.value)} />
              <input style={{ ...styles.input, flex:1 }} type="number" min={1} required
                placeholder="Số lượng"
                value={item.quantity}
                onChange={e => setItem(i, 'quantity', Number(e.target.value))} />
              <input style={{ ...styles.input, flex:1 }} type="number" min={0.01} step={0.01} required
                placeholder="KG"
                value={item.weight}
                onChange={e => setItem(i, 'weight', Number(e.target.value))} />
              {items.length > 1 &&
                <button type="button" style={styles.btnRemove} onClick={() => removeItem(i)}>✕</button>}
            </div>
          ))}

          <button type="submit" style={styles.btnSubmit} disabled={loading}>
            {loading ? 'Đang tạo...' : '🚀 Tạo đơn hàng'}
          </button>
        </form>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  page:     { padding:24, maxWidth:900, margin:'0 auto' },
  card:     { background:'#fff', borderRadius:12, padding:32, boxShadow:'0 2px 12px rgba(0,0,0,0.08)' },
  header:   { display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:24 },
  section:  { margin:'20px 0 12px', color:'#444' },
  row:      { display:'flex', gap:16 },
  col:      { flex:1 },
  label:    { display:'block', fontSize:13, color:'#555', marginBottom:6 },
  input:    { width:'100%', padding:'10px 12px', border:'1px solid #ddd', borderRadius:6, fontSize:14, boxSizing:'border-box' },
  itemRow:  { display:'flex', gap:8, alignItems:'center', marginTop:8 },
  btnAdd:   { padding:'6px 14px', background:'#f0f7ff', color:'#1677ff', border:'1px solid #91caff', borderRadius:6, cursor:'pointer', fontSize:13 },
  btnRemove:{ padding:'6px 10px', background:'#fff2f0', color:'#f5222d', border:'1px solid #ffccc7', borderRadius:6, cursor:'pointer' },
  btnBack:  { padding:'8px 16px', background:'#fff', border:'1px solid #ddd', borderRadius:6, cursor:'pointer', fontSize:13 },
  btnSubmit:{ marginTop:28, width:'100%', padding:'13px', background:'#1677ff', color:'#fff', border:'none', borderRadius:8, fontSize:15, cursor:'pointer', fontWeight:600 },
}
