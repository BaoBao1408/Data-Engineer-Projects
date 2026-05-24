import { Component, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService, OrderResponse, PagedResult } from '../../services/api.service';

@Component({
  selector: 'app-orders',
  standalone: true,
  imports: [CommonModule],
  template: `
    <div class="page">
      <h2>📦 Orders</h2>
      <div *ngIf="loading">Loading...</div>
      <div *ngIf="!loading">
        <p>Total: {{ result?.total }} orders</p>
        <table>
          <thead>
            <tr>
              <th>Code</th><th>Customer</th>
              <th>Status</th><th>Fee</th><th>Created</th>
            </tr>
          </thead>
          <tbody>
            <tr *ngFor="let o of result?.data">
              <td>{{ o.orderCode }}</td>
              <td>{{ o.customerName }}</td>
              <td>
                <span [class]="'badge badge-' + o.status.toLowerCase()">
                  {{ o.status }}
                </span>
              </td>
              <td>{{ o.shippingFee | number }}</td>
              <td>{{ o.createdAt | date:'short' }}</td>
            </tr>
          </tbody>
        </table>
        <div class="pagination">
          <button (click)="prevPage()" [disabled]="page <= 1">←</button>
          <span>Page {{ page }} / {{ result?.totalPages }}</span>
          <button (click)="nextPage()" [disabled]="page >= (result?.totalPages ?? 1)">→</button>
        </div>
      </div>
    </div>
  `
})
export class OrdersComponent implements OnInit {
  result: PagedResult<OrderResponse> | null = null;
  page = 1;
  loading = false;

  constructor(private api: ApiService) {}

  ngOnInit() { this.load(); }

  load() {
    this.loading = true;
    this.api.getOrders(this.page).subscribe({
      next: (r) => { this.result = r; this.loading = false; },
      error: () => { this.loading = false; }
    });
  }

  prevPage() { if (this.page > 1) { this.page--; this.load(); } }
  nextPage() {
    if (this.page < (this.result?.totalPages ?? 1)) {
      this.page++;
      this.load();
    }
  }
}
