export type OrderStatus =
  | 'Pending' | 'Confirmed' | 'PickedUp'
  | 'InTransit' | 'Delivered' | 'Cancelled'

export interface OrderItem {
  id: string; productName: string; quantity: number; weight: number
}
export interface Order {
  id: string; orderCode: string; customerName: string; customerPhone: string
  originAddress: string; destinationAddress: string
  totalWeight: number; shippingFee: number
  status: OrderStatus; createdAt: string; items: OrderItem[]
}
export interface CreateOrderRequest {
  customerName: string; customerPhone: string
  originAddress: string; destinationAddress: string
  items: { productName: string; quantity: number; weight: number }[]
}
export interface PagedResult<T> {
  data: T[]; total: number; page: number; pageSize: number; totalPages: number
}
export interface TrackingEvent {
  status: string; location: string; note: string; occurredAt: string
}
export interface Tracking {
  orderId: string; orderCode: string; customerName: string
  currentStatus: string; createdAt: string; timeline: TrackingEvent[]
}
export interface LoginResponse { token: string; expiresIn: number }
