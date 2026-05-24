import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService, TrackingResponse } from '../../services/api.service';

@Component({
  selector: 'app-tracking',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="page">
      <h2>🗺️ Track Shipment</h2>
      <div class="search">
        <input [(ngModel)]="orderCode" placeholder="Enter order code (e.g. SML-20240101-ABC123)" />
        <button (click)="search()" [disabled]="loading">Track</button>
      </div>

      <div *ngIf="tracking" class="result">
        <h3>{{ tracking.orderCode }}</h3>
        <p>Customer: {{ tracking.customerName }}</p>
        <p>Status: <strong>{{ tracking.currentStatus }}</strong></p>

        <div class="timeline">
          <div *ngFor="let e of tracking.timeline" class="event">
            <div class="dot"></div>
            <div class="info">
              <strong>{{ e.status }}</strong> — {{ e.location }}
              <p>{{ e.note }}</p>
              <small>{{ e.occurredAt | date:'medium' }}</small>
            </div>
          </div>
        </div>
      </div>

      <p *ngIf="notFound" class="error">Order not found</p>
    </div>
  `
})
export class TrackingComponent {
  orderCode = '';
  tracking: TrackingResponse | null = null;
  loading = false;
  notFound = false;

  constructor(private api: ApiService) {}

  search() {
    if (!this.orderCode) return;
    this.loading = true;
    this.notFound = false;
    this.api.getTracking(this.orderCode).subscribe({
      next: (r) => { this.tracking = r; this.loading = false; },
      error: () => { this.notFound = true; this.loading = false; }
    });
  }
}
