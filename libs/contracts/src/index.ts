export const TOPICS = {
  activationRequested: 'activation.requested',
  billingEvents: 'billing.events',
  provisioningEvents: 'provisioning.events',
  activationEvents: 'activation.events',
} as const;

export type TopicName = (typeof TOPICS)[keyof typeof TOPICS];
export type DeadLetterTopic = `${TopicName}.dlq`;

export const DEAD_LETTER_TOPICS: Record<TopicName, DeadLetterTopic> = {
  [TOPICS.activationRequested]: 'activation.requested.dlq',
  [TOPICS.billingEvents]: 'billing.events.dlq',
  [TOPICS.provisioningEvents]: 'provisioning.events.dlq',
  [TOPICS.activationEvents]: 'activation.events.dlq',
};

export enum ActivationStatus {
  PENDING = 'PENDING',
  IN_PROGRESS = 'IN_PROGRESS',
  ACTIVE = 'ACTIVE',
  FAILED = 'FAILED',
}

export type SimulateFailure = 'none' | 'billing' | 'provisioning';

export interface EventEnvelope<TPayload, TEventType extends string = string> {
  eventId: string;
  eventType: TEventType;
  version: number;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: string;
  payload: TPayload;
}

export interface EventContext {
  activationId: string;
  customerId: string;
  source: string;
  version?: number;
  occurredAt?: string;
  eventId?: string;
}

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: SimulateFailure;
}

// DECISIÓN: los payloads no detallados en el documento usan solo el dato mínimo del flujo.
export interface BillingAccountCreatedPayload {
  accountStatus: 'CREATED';
}

export interface BillingFailedPayload {
  reason: string;
}

export interface BillingAccountCancelledPayload {
  accountStatus: 'CANCELLED';
  reason: string;
}

export interface ProvisioningCompletedPayload {
  status: 'COMPLETED';
}

export interface ProvisioningFailedPayload {
  reason: string;
}

export interface ActivationCompletedPayload {
  status: ActivationStatus.ACTIVE;
}

export interface ActivationFailedPayload {
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

export type ActivationRequestedEvent = EventEnvelope<ActivationRequestedPayload, 'ActivationRequested'>;
export type BillingAccountCreatedEvent = EventEnvelope<BillingAccountCreatedPayload, 'BillingAccountCreated'>;
export type BillingFailedEvent = EventEnvelope<BillingFailedPayload, 'BillingFailed'>;
export type BillingAccountCancelledEvent = EventEnvelope<BillingAccountCancelledPayload, 'BillingAccountCancelled'>;
export type ProvisioningCompletedEvent = EventEnvelope<ProvisioningCompletedPayload, 'ProvisioningCompleted'>;
export type ProvisioningFailedEvent = EventEnvelope<ProvisioningFailedPayload, 'ProvisioningFailed'>;
export type ActivationCompletedEvent = EventEnvelope<ActivationCompletedPayload, 'ActivationCompleted'>;
export type ActivationFailedEvent = EventEnvelope<ActivationFailedPayload, 'ActivationFailed'>;

export type ActivationEvent =
  | ActivationRequestedEvent
  | BillingAccountCreatedEvent
  | BillingFailedEvent
  | BillingAccountCancelledEvent
  | ProvisioningCompletedEvent
  | ProvisioningFailedEvent
  | ActivationCompletedEvent
  | ActivationFailedEvent;

function createEvent<TPayload, TEventType extends ActivationEvent['eventType']>(
  context: EventContext,
  eventType: TEventType,
  payload: TPayload,
): EventEnvelope<TPayload, TEventType> {
  return {
    eventId: context.eventId ?? globalThis.crypto.randomUUID(),
    eventType,
    version: context.version ?? 1,
    occurredAt: context.occurredAt ?? new Date().toISOString(),
    correlationId: context.activationId,
    customerId: context.customerId,
    source: context.source,
    payload,
  };
}

export function createActivationRequested(
  context: EventContext,
  payload: ActivationRequestedPayload,
): ActivationRequestedEvent {
  return createEvent(context, 'ActivationRequested', payload);
}

export function createBillingAccountCreated(
  context: EventContext,
  payload: BillingAccountCreatedPayload,
): BillingAccountCreatedEvent {
  return createEvent(context, 'BillingAccountCreated', payload);
}

export function createBillingFailed(
  context: EventContext,
  payload: BillingFailedPayload,
): BillingFailedEvent {
  return createEvent(context, 'BillingFailed', payload);
}

export function createBillingAccountCancelled(
  context: EventContext,
  payload: BillingAccountCancelledPayload,
): BillingAccountCancelledEvent {
  return createEvent(context, 'BillingAccountCancelled', payload);
}

export function createProvisioningCompleted(
  context: EventContext,
  payload: ProvisioningCompletedPayload,
): ProvisioningCompletedEvent {
  return createEvent(context, 'ProvisioningCompleted', payload);
}

export function createProvisioningFailed(
  context: EventContext,
  payload: ProvisioningFailedPayload,
): ProvisioningFailedEvent {
  return createEvent(context, 'ProvisioningFailed', payload);
}

export function createActivationCompleted(
  context: EventContext,
  payload: ActivationCompletedPayload,
): ActivationCompletedEvent {
  return createEvent(context, 'ActivationCompleted', payload);
}

export function createActivationFailed(
  context: EventContext,
  payload: ActivationFailedPayload,
): ActivationFailedEvent {
  return createEvent(context, 'ActivationFailed', payload);
}
