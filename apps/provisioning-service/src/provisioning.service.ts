import {
  DEAD_LETTER_TOPICS,
  TOPICS,
  createProvisioningCompleted,
  createProvisioningFailed,
  isActivationEvent,
  type ActivationEvent,
  type ActivationRequestedEvent,
} from '@activation-poc/contracts';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, type Consumer, type EachMessagePayload, type Producer } from 'kafkajs';
import { connect, model, Schema } from 'mongoose';

type ProvisioningStatus = 'COMPLETED' | 'FAILED';

interface ProvisioningOrder {
  activationId: string;
  eventId: string;
  customerId: string;
  planId: string;
  status: ProvisioningStatus;
}

const provisioningOrderSchema = new Schema<ProvisioningOrder>({
  activationId: { type: String, required: true, unique: true },
  eventId: { type: String, required: true },
  customerId: { type: String, required: true },
  planId: { type: String, required: true },
  status: { type: String, enum: ['COMPLETED', 'FAILED'], required: true },
}, { timestamps: true, collection: 'provisioning_orders' });

const ProvisioningOrderModel = model<ProvisioningOrder>('ProvisioningOrder', provisioningOrderSchema);
const retryDelaysMs = [1_000, 2_000, 4_000];

@Injectable()
export class ProvisioningService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ProvisioningService.name);
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'provisioning-service',
    brokers: (process.env.KAFKA_BROKERS ?? process.env.KAFKA_BROKER ?? 'localhost:9092')
      .split(',').map((broker) => broker.trim()).filter(Boolean),
  });
  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID ?? 'provisioning-service',
  });
  private readonly producer: Producer = this.kafka.producer();

  async onModuleInit(): Promise<void> {
    await connect(
      process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation_db?directConnection=true',
    );
    await ProvisioningOrderModel.init();
    await Promise.all([this.consumer.connect(), this.producer.connect()]);
    await this.consumer.subscribe({ topic: TOPICS.activationRequested, fromBeginning: false });
    await this.consumer.run({ eachMessage: async (payload) => this.processWithRetries(payload) });
    this.logger.log(`Listening for ${TOPICS.activationRequested}`);
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
        if (!isActivationEvent(event) || event.eventType !== 'ActivationRequested') {
          throw new Error('Invalid ActivationRequested event');
        }
        await this.handleRequest(event);
        return;
      } catch (error) {
        lastError = error;
        const delay = retryDelaysMs[attempt];
        if (delay === undefined) break;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

    await this.producer.send({
      topic: DEAD_LETTER_TOPICS[TOPICS.activationRequested],
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
    this.logger.error('Moved failed request to activation.requested.dlq', lastError instanceof Error ? lastError.stack : String(lastError));
  }

  private async handleRequest(event: ActivationRequestedEvent): Promise<void> {
    await new Promise((resolve) => setTimeout(resolve, 1_000 + Math.floor(Math.random() * 2_001)));
    const order = await ProvisioningOrderModel.findOneAndUpdate(
      { activationId: event.correlationId },
      {
        $setOnInsert: {
          activationId: event.correlationId,
          eventId: event.eventId,
          customerId: event.customerId,
          planId: event.payload.planId,
          status: event.payload.simulateFailure === 'provisioning' ? 'FAILED' : 'COMPLETED',
        },
      },
      { upsert: true, new: true },
    ).exec();

    const result: ActivationEvent = order.status === 'FAILED'
      ? createProvisioningFailed({
        activationId: event.correlationId,
        customerId: event.customerId,
        source: 'provisioning-service',
        eventId: `${event.correlationId}:ProvisioningFailed`,
      }, { reason: 'Fallo de aprovisionamiento simulado' })
      : createProvisioningCompleted({
        activationId: event.correlationId,
        customerId: event.customerId,
        source: 'provisioning-service',
        eventId: `${event.correlationId}:ProvisioningCompleted`,
      }, { status: 'COMPLETED' });
    await this.producer.send({
      topic: TOPICS.provisioningEvents,
      messages: [{ key: event.customerId, value: JSON.stringify(result) }],
    });
    this.logger.log(`Processed provisioning for ${event.correlationId}`);
  }
}
