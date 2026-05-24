import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface LoginResponse { token: string; expiresIn: number; }
export interface OrderResponse {
  id: string; orderCode: string; customerName: string;
  customerPhone: string; originAddress: string;
  destinationAddress: string; totalWeight: number;
  shippingFee: number; status: string; createdAt: string;
  items: OrderItemResponse[];
}
export interface OrderItemResponse {
  id: string; productName: string; quantity: number; weight: number;
}
export interface PagedResult<T> {
  data: T[]; total: number; page: number;
  pageSize: number; totalPages: number;
}
export interface TrackingResponse {
  orderId: string; orderCode: string; customerName: string;
  currentStatus: string; createdAt: string;
  timeline: { status: string; location: string; note: string; occurredAt: string; }[];
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  private orderUrl = environment.orderServiceUrl;
  private trackUrl = environment.trackingServiceUrl;

  constructor(private http: HttpClient) {}

  private get headers(): HttpHeaders {
    return new HttpHeaders({
      Authorization: `Bearer ${localStorage.getItem('token') ?? ''}`
    });
  }

  // ── Auth ──────────────────────────────────────────────
  login(username: string, password: string): Observable<LoginResponse> {
    return this.http.post<LoginResponse>(
      `${this.orderUrl}/auth/login`, { username, password });
  }

  // ── Orders ────────────────────────────────────────────
  getOrders(page = 1, size = 10): Observable<PagedResult<OrderResponse>> {
    return this.http.get<PagedResult<OrderResponse>>(
      `${this.orderUrl}/orders?page=${page}&pageSize=${size}`,
      { headers: this.headers });
  }

  getOrder(id: string): Observable<OrderResponse> {
    return this.http.get<OrderResponse>(
      `${this.orderUrl}/orders/${id}`, { headers: this.headers });
  }

  createOrder(body: any): Observable<OrderResponse> {
    return this.http.post<OrderResponse>(
      `${this.orderUrl}/orders`, body, { headers: this.headers });
  }

  updateStatus(id: string, status: string): Observable<OrderResponse> {
    return this.http.patch<OrderResponse>(
      `${this.orderUrl}/orders/${id}/status`,
      { status }, { headers: this.headers });
  }

  uploadFile(orderId: string, file: File): Observable<any> {
    const fd = new FormData();
    fd.append('file', file);
    return this.http.post(
      `${this.orderUrl}/orders/${orderId}/attachments`, fd,
      { headers: new HttpHeaders({ Authorization: `Bearer ${localStorage.getItem('token')}` }) });
  }

  // ── Tracking ──────────────────────────────────────────
  getTracking(orderCode: string): Observable<TrackingResponse> {
    return this.http.get<TrackingResponse>(
      `${this.trackUrl}/tracking/${orderCode}`);
  }
}
