import {
  DEAD_LETTER_TOPICS,
  TOPICS,
  ActivationStatus,
  isActivationEvent,
  type ActivationFailedEvent,
  type ActivationRequestedEvent,
} from '@activation-poc/contracts';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Kafka, type Consumer, type EachMessagePayload, type Producer } from 'kafkajs';
import { Model } from 'mongoose';
import { Activation, ActivationDocument } from '../schemas/activation.schema.js';

const retryDelaysMs = [1_000, 2_000, 4_000];

@Injectable()
export class ActivationConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ActivationConsumer.name);
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'activation-worker',
    brokers: (process.env.KAFKA_BROKERS ?? process.env.KAFKA_BROKER ?? 'localhost:9092')
      .split(',').map((broker) => broker.trim()).filter(Boolean),
  });
  private readonly consumer: Consumer;
  private readonly producer: Producer;

  constructor(
    @InjectModel(Activation.name) private readonly activationModel: Model<ActivationDocument>,
  ) {
    this.consumer = this.kafka.consumer({
      groupId: process.env.KAFKA_GROUP_ID ?? 'activation-worker',
    });
    this.producer = this.kafka.producer();
  }

  async onModuleInit(): Promise<void> {
    await this.activationModel.init();
    await Promise.all([this.consumer.connect(), this.producer.connect()]);
    await Promise.all([
      this.consumer.subscribe({
        topic: TOPICS.activationRequested,
        fromBeginning: process.env.KAFKA_FROM_BEGINNING === 'true',
      }),
      this.consumer.subscribe({ topic: TOPICS.activationEvents, fromBeginning: false }),
    ]);
    await this.consumer.run({
      eachMessage: async (payload) => this.handleMessage(payload),
    });
    this.logger.log(`Subscribed to ${TOPICS.activationRequested} and ${TOPICS.activationEvents}`);
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.all([this.consumer.disconnect(), this.producer.disconnect()]);
  }

  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= retryDelaysMs.length; attempt += 1) {
      try {
        const value = payload.message.value?.toString();
        if (!value) throw new Error('Kafka message has no value');
        const parsed: unknown = JSON.parse(value);
        if (!isActivationEvent(parsed)) throw new Error('Invalid activation event envelope');
        if (payload.topic === TOPICS.activationRequested
          && parsed.eventType !== 'ActivationRequested') {
          throw new Error('Expected ActivationRequested on activation.requested');
        }
        if (payload.topic === TOPICS.activationEvents
          && parsed.eventType !== 'ActivationFailed') {
          return;
        }

        if (parsed.eventType === 'ActivationRequested') {
          await this.handleActivationRequested(parsed);
        } else if (parsed.eventType === 'ActivationFailed') {
          await this.handleActivationFailed(parsed);
        } else {
          return;
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
    this.logger.error(
      `Moved failed message to ${DEAD_LETTER_TOPICS[sourceTopic]}`,
      lastError instanceof Error ? lastError.stack : String(lastError),
    );
  }

  private async handleActivationRequested(event: ActivationRequestedEvent): Promise<void> {
    await this.activationModel.findOneAndUpdate(
      { eventId: event.eventId },
      {
        $setOnInsert: {
          activationId: event.correlationId,
          eventId: event.eventId,
          customerId: event.customerId,
          planId: event.payload.planId,
          channel: event.payload.channel,
          simulateFailure: event.payload.simulateFailure,
          status: ActivationStatus.PENDING,
          history: [event],
          processedEventIds: [],
        },
      },
      { upsert: true, new: true, runValidators: true },
    ).exec();

    const alreadyProcessed = await this.activationModel.exists({
      activationId: event.correlationId,
      processedEventIds: event.eventId,
    });
    if (alreadyProcessed) return;

    await this.activationModel.updateOne(
      { activationId: event.correlationId },
      { $addToSet: { processedEventIds: event.eventId } },
    ).exec();
    this.logger.log(`Persisted activation request ${event.correlationId}`);
  }

  private async handleActivationFailed(
    event: ActivationFailedEvent,
  ): Promise<void> {
    const activation = await this.activationModel.findOne({ activationId: event.correlationId }).exec();
    if (!activation) throw new Error(`Activation ${event.correlationId} was not found for compensation`);
    if (activation.processedEventIds.includes(event.eventId)) return;

    await this.activationModel.updateOne(
      { activationId: event.correlationId },
      {
        $set: { status: ActivationStatus.FAILED },
        $addToSet: {
          processedEventIds: event.eventId,
          history: event,
        },
      },
    ).exec();
    this.logger.log(`Recorded failed activation ${event.correlationId}`);
  }
}
