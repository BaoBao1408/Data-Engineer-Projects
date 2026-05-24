import { inject } from '@angular/core';
import { Router } from '@angular/router';

export const authGuard = () => {
  const token = localStorage.getItem('token');
  if (token) return true;
  inject(Router).navigate(['/login']);
  return false;
};
