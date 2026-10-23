import assert from 'node:assert/strict';
import test from 'node:test';
import { EVENT_VERSION, isActivationEvent } from '../dist/index.js';

const envelope = {
  eventId: 'event-001',
  version: EVENT_VERSION,
  occurredAt: '2026-10-23T12:00:00.000Z',
  correlationId: 'activation-001',
  customerId: 'customer-001',
  source: 'contract-test',
};

const validEvents = [
  ['ActivationRequested', { planId: 'FIBRA-300', channel: 'web', simulateFailure: 'none' }],
  ['BillingAccountCreated', { accountStatus: 'CREATED' }],
  ['BillingFailed', { reason: 'billing unavailable' }],
  ['BillingAccountCancelled', { accountStatus: 'CANCELLED', reason: 'saga compensation' }],
  ['ProvisioningCompleted', { status: 'COMPLETED' }],
  ['ProvisioningFailed', { reason: 'provisioning unavailable' }],
  ['ActivationCompleted', { status: 'ACTIVE' }],
  ['ActivationFailed', { failedStep: 'provisioning', reason: 'provisioning unavailable' }],
];

for (const [eventType, payload] of validEvents) {
  test(`accepts ${eventType} with its contract payload`, () => {
    assert.equal(isActivationEvent({ ...envelope, eventType, payload }), true);
  });
}

test('rejects an unsupported event version', () => {
  assert.equal(isActivationEvent({
    ...envelope,
    version: EVENT_VERSION + 1,
    eventType: 'ActivationCompleted',
    payload: { status: 'ACTIVE' },
  }), false);
});

test('rejects unknown event types and malformed payloads', () => {
  assert.equal(isActivationEvent({ ...envelope, eventType: 'UnknownEvent', payload: {} }), false);
  assert.equal(isActivationEvent({
    ...envelope,
    eventType: 'ActivationFailed',
    payload: { failedStep: 'billing', reason: 4 },
  }), false);
});

test('rejects an invalid timestamp or incomplete envelope', () => {
  assert.equal(isActivationEvent({
    ...envelope,
    occurredAt: 'yesterday',
    eventType: 'ActivationCompleted',
    payload: { status: 'ACTIVE' },
  }), false);
  assert.equal(isActivationEvent({ eventType: 'ActivationCompleted' }), false);
});