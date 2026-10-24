import {
  DEAD_LETTER_TOPICS,
  TOPICS,
  createBillingAccountCancelled,
  createBillingAccountCreated,
  createBillingFailed,
  isActivationEvent,
  type ActivationEvent,
  type ActivationFailedEvent,
  type ActivationRequestedEvent,
} from '@activation-poc/contracts';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, type Consumer, type EachMessagePayload, type Producer } from 'kafkajs';
import { connect, model, Schema } from 'mongoose';

type BillingStatus = 'CREATED' | 'CANCELLED' | 'FAILED';

interface BillingAccount {
  activationId: string;
  eventId: string;
  customerId: string;
  planId: string;
  status: BillingStatus;
}

const billingAccountSchema = new Schema<BillingAccount>({
  activationId: { type: String, required: true, unique: true },
  eventId: { type: String, required: true },
  customerId: { type: String, required: true },
  planId: { type: String, required: true },
  status: { type: String, enum: ['CREATED', 'CANCELLED', 'FAILED'], required: true },
}, { timestamps: true, collection: 'billing_accounts' });

const BillingAccountModel = model<BillingAccount>('BillingAccount', billingAccountSchema);
const retryDelaysMs = [1_000, 2_000, 4_000];

@Injectable()
export class BillingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(BillingService.name);
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'billing-service',
    brokers: (process.env.KAFKA_BROKERS ?? process.env.KAFKA_BROKER ?? 'localhost:9092')
      .split(',').map((broker) => broker.trim()).filter(Boolean),
  });
  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID ?? 'billing-service',
  });
  private readonly producer: Producer = this.kafka.producer();

  async onModuleInit(): Promise<void> {
    await connect(
      process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation_db?directConnection=true',
    );
    await BillingAccountModel.init();
    await Promise.all([this.consumer.connect(), this.producer.connect()]);
    await Promise.all([
      this.consumer.subscribe({ topic: TOPICS.activationRequested, fromBeginning: false }),
      this.consumer.subscribe({ topic: TOPICS.activationEvents, fromBeginning: false }),
    ]);
    await this.consumer.run({ eachMessage: async (payload) => this.processWithRetries(payload) });
    this.logger.log('Listening for activation requests and saga failures');
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.all([this.consumer.disconnect(), this.producer.disconnect()]);
  }

  private async processWithRetries(payload: EachMessagePayload): Promise<void> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= retryDelaysMs.length; attempt += 1) {
      try {
        const value = payload.message.value?.toString();
        if (!value) throw new Error('Kafka message has no value');
        const event: unknown = JSON.parse(value);
        if (!isActivationEvent(event)) throw new Error('Invalid activation event');

        if (payload.topic === TOPICS.activationRequested
          && event.eventType === 'ActivationRequested') {
          await this.handleRequest(event);
        } else if (payload.topic === TOPICS.activationEvents
          && event.eventType === 'ActivationFailed') {
          await this.compensate(event);
        }
        return;
      } catch (error) {
        lastError = error;
        const delay = retryDelaysMs[attempt];
        if (delay === undefined) break;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

    const sourceTopic = payload.topic === TOPICS.activationRequested
      ? TOPICS.activationRequested
      : TOPICS.activationEvents;
    await this.producer.send({
      topic: DEAD_LETTER_TOPICS[sourceTopic],
      messages: [{
        key: payload.message.key?.toString() ?? null,
        value: payload.message.value,
        headers: {
          'x-original-topic': payload.topic,
          'x-original-partition': String(payload.partition),
          'x-original-offset': payload.message.offset,
          'x-error-message': lastError instanceof Error ? lastError.message : String(lastError),
        },
      }],
    });
    this.logger.error(`Moved failed event to ${DEAD_LETTER_TOPICS[sourceTopic]}`, lastError instanceof Error ? lastError.stack : String(lastError));
  }

  private async handleRequest(event: ActivationRequestedEvent): Promise<void> {
    const account = await BillingAccountModel.findOneAndUpdate(
      { activationId: event.correlationId },
      {
        $setOnInsert: {
          activationId: event.correlationId,
          eventId: event.eventId,
          customerId: event.customerId,
          planId: event.payload.planId,
          status: event.payload.simulateFailure === 'billing' ? 'FAILED' : 'CREATED',
        },
      },
      { upsert: true, new: true },
    ).exec();

    if (account.status === 'CANCELLED') return;
    const result = account.status === 'FAILED'
      ? createBillingFailed({
        activationId: event.correlationId,
        customerId: event.customerId,
        source: 'billing-service',
        eventId: `${event.correlationId}:BillingFailed`,
      }, { reason: 'Fallo de facturación simulado' })
      : createBillingAccountCreated({
        activationId: event.correlationId,
        customerId: event.customerId,
        source: 'billing-service',
        eventId: `${event.correlationId}:BillingAccountCreated`,
      }, { accountStatus: 'CREATED' });
    await this.publish(result);
  }

  private async compensate(event: ActivationFailedEvent): Promise<void> {
    const account = await BillingAccountModel.findOne({ activationId: event.correlationId }).exec();
    if (account?.status === 'FAILED') return;
    await BillingAccountModel.updateOne(
      { activationId: event.correlationId },
      {
        $setOnInsert: {
          activationId: event.correlationId,
          eventId: event.eventId,
          customerId: event.customerId,
          planId: 'unknown',
        },
        $set: { status: 'CANCELLED' },
      },
      { upsert: true },
    ).exec();
    const cancellation = createBillingAccountCancelled({
      activationId: event.correlationId,
      customerId: event.customerId,
      source: 'billing-service',
      eventId: `${event.correlationId}:BillingAccountCancelled`,
    }, {
      accountStatus: 'CANCELLED',
      reason: `Compensación por fallo de ${event.payload.failedStep}: ${event.payload.reason}`,
    });
    await this.publish(cancellation);
    this.logger.log(`Compensated billing for ${event.correlationId}`);
  }

  private async publish(event: ActivationEvent): Promise<void> {
    await this.producer.send({
      topic: TOPICS.billingEvents,
      messages: [{ key: event.customerId, value: JSON.stringify(event) }],
    });
  }
}
