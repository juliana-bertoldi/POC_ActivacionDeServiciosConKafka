export interface ActivationRequestedEvent {
  eventId: string;
  userId: string;
  planId: string;
  timestamp: string;
}

export interface ActivationCompletedEvent {
  eventId: string;
  userId: string;
  status: 'SUCCESS' | 'FAILED';
  reason?: string;
  timestamp: string;
}

export interface BillingProcessedEvent {
  eventId: string;
  userId: string;
  amount: number;
  status: 'PAID' | 'REJECTED';
  timestamp: string;
}