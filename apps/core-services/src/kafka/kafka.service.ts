import {
  ActivationStatus,
  DEAD_LETTER_TOPICS,
  EVENT_VERSION,
  TOPICS,
  createActivationCompleted,
  createActivationFailed,
  createActivationRequested,
  isActivationEvent,
  type ActivationEvent,
  type ActivationFailedEvent,
  type ActivationRequestedEvent,
  type CreateActivationRequest,
} from '@activation-poc/contracts';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Kafka, type Consumer, type EachMessagePayload, type Producer } from 'kafkajs';
import { Model } from 'mongoose';
import { Activation, ProcessedEvent, type ActivationDocument } from '../schemas/activation.schema.js';
import { ActivationGateway } from '../websocket/activation.gateway.js';

const retryDelaysMs = [1_000, 2_000, 4_000];
type BusinessResultEvent = Extract<ActivationEvent, {
  eventType: 'BillingAccountCreated' | 'BillingFailed' | 'BillingAccountCancelled'
    | 'ProvisioningCompleted' | 'ProvisioningFailed';
}>;

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'core-services',
    brokers: (process.env.KAFKA_BROKERS ?? process.env.KAFKA_BROKER ?? 'localhost:9092')
      .split(',').map((broker) => broker.trim()).filter(Boolean),
  });
  private readonly producer: Producer = this.kafka.producer();
  private readonly consumer: Consumer = this.kafka.consumer({
    groupId: `${process.env.KAFKA_GROUP_ID ?? 'core-services'}-saga`,
  });

  constructor(
    @InjectModel(Activation.name) private readonly activations: Model<ActivationDocument>,
    @InjectModel(ProcessedEvent.name) private readonly processedEvents: Model<ProcessedEvent>,
    private readonly gateway: ActivationGateway,
  ) {}

  async onModuleInit(): Promise<void> {
    await Promise.all([this.producer.connect(), this.consumer.connect()]);
    await this.consumer.subscribe({ topic: TOPICS.billingEvents, fromBeginning: false });
    await this.consumer.subscribe({ topic: TOPICS.provisioningEvents, fromBeginning: false });
    await this.consumer.run({
      eachMessage: async (payload) => this.handleResultMessage(payload),
    });
    this.logger.log('Connected to Kafka and subscribed to saga result topics');
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.all([this.consumer.disconnect(), this.producer.disconnect()]);
  }

  async createActivation(request: CreateActivationRequest): Promise<ActivationRequestedEvent> {
    const activation = createActivationRequested({
      activationId: globalThis.crypto.randomUUID(),
      customerId: request.customerId.trim(),
      source: 'core-services',
      version: EVENT_VERSION,
    }, {
      planId: request.planId.trim(),
      channel: request.channel.trim(),
      simulateFailure: request.simulateFailure,
    });

    const record = new this.activations({
      activationId: activation.correlationId,
      eventId: activation.eventId,
      customerId: activation.customerId,
      planId: activation.payload.planId,
      channel: activation.payload.channel,
      simulateFailure: activation.payload.simulateFailure,
      status: ActivationStatus.PENDING,
      history: [activation],
      results: { billing: null, provisioning: null },
      finalEvent: null,
    });
    await record.save();
    await this.publishEvent(TOPICS.activationRequested, activation);
    this.gateway.publish(activation);
    this.logger.log(
      `Published ${activation.eventType} eventId=${activation.eventId} customerId=${activation.customerId}`,
    );
    return activation;
  }

  async findActivation(activationId: string): Promise<ActivationDocument | null> {
    return this.activations.findOne({ activationId }).exec();
  }

  private async handleResultMessage(payload: EachMessagePayload): Promise<void> {
    let lastError: unknown;
    for (let attempt = 0; attempt <= retryDelaysMs.length; attempt += 1) {
      try {
        const value = payload.message.value?.toString();
        if (!value) throw new Error('Kafka result message has no value');
        const parsed: unknown = JSON.parse(value);
        if (!isActivationEvent(parsed) || !this.isBusinessResultEvent(parsed)) {
          throw new Error('Kafka message is not a valid saga result event');
        }
        await this.processResult(parsed);
        return;
      } catch (error) {
        lastError = error;
        const delay = retryDelaysMs[attempt];
        if (delay === undefined) break;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

    const topic = payload.topic === TOPICS.billingEvents
      ? TOPICS.billingEvents
      : TOPICS.provisioningEvents;
    await this.producer.send({
      topic: DEAD_LETTER_TOPICS[topic],
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
    this.logger.error(`Moved failed event to ${DEAD_LETTER_TOPICS[topic]}`, lastError instanceof Error ? lastError.stack : String(lastError));
  }

  private isBusinessResultEvent(event: ActivationEvent): event is BusinessResultEvent {
    return event.eventType === 'BillingAccountCreated'
      || event.eventType === 'BillingFailed'
      || event.eventType === 'ProvisioningCompleted'
      || event.eventType === 'ProvisioningFailed';
  }

  private async processResult(event: BusinessResultEvent): Promise<void> {
    const alreadyProcessed = await this.processedEvents.exists({ eventId: event.eventId });
    if (alreadyProcessed) {
      if (event.eventType !== 'BillingAccountCancelled') {
        await this.publishFinalEvent(event.correlationId, event.customerId);
      }
      return;
    }
    const activation = await this.activations.findOne({ activationId: event.correlationId }).exec();
    if (!activation) throw new Error(`Activation ${event.correlationId} was not found`);

    await this.activations.updateOne(
      { activationId: event.correlationId, 'history.eventId': { $ne: event.eventId } },
      { $push: { history: event } },
    ).exec();
    if (event.eventType !== 'BillingAccountCancelled') {
      const step = event.eventType.startsWith('Billing') ? 'billing' : 'provisioning';
      const isFailure = event.eventType === 'BillingFailed' || event.eventType === 'ProvisioningFailed';
      await this.activations.updateOne(
        { activationId: event.correlationId },
        { $set: { [`results.${step}`]: isFailure ? 'FAILED' : 'OK' } },
      ).exec();
      await this.activations.updateOne(
        {
          activationId: event.correlationId,
          status: { $nin: [ActivationStatus.ACTIVE, ActivationStatus.FAILED] },
        },
        { $set: { status: ActivationStatus.IN_PROGRESS } },
      ).exec();
    }
    await this.processedEvents.updateOne(
      { eventId: event.eventId },
      { $setOnInsert: { eventId: event.eventId } },
      { upsert: true },
    ).exec();

    if (event.eventType !== 'BillingAccountCancelled') {
      await this.publishFinalEvent(event.correlationId, event.customerId);
    }
    this.gateway.publish(event);
  }

  private async publishFinalEvent(activationId: string, customerId: string): Promise<void> {
    const activation = await this.activations.findOne({ activationId }).exec();
    if (!activation) return;

    let finalEvent = activation.finalEvent ?? undefined;
    if (!finalEvent && activation.results.billing === 'FAILED') {
      const failure = activation.history.find((event) => event.eventType === 'BillingFailed');
      if (failure?.eventType !== 'BillingFailed') return;
      finalEvent = createActivationFailed({
        activationId,
        customerId,
        source: 'core-services',
        eventId: `${activationId}:ActivationFailed`,
      }, {
        failedStep: 'billing',
        reason: failure.payload.reason,
      });
    } else if (!finalEvent && activation.results.provisioning === 'FAILED') {
      const failure = activation.history.find((event) => event.eventType === 'ProvisioningFailed');
      if (failure?.eventType !== 'ProvisioningFailed') return;
      finalEvent = createActivationFailed({
        activationId,
        customerId,
        source: 'core-services',
        eventId: `${activationId}:ActivationFailed`,
      }, {
        failedStep: 'provisioning',
        reason: failure.payload.reason,
      });
    } else if (!finalEvent && activation.results.billing === 'OK' && activation.results.provisioning === 'OK') {
      finalEvent = createActivationCompleted({
        activationId,
        customerId,
        source: 'core-services',
        eventId: `${activationId}:ActivationCompleted`,
      }, { status: ActivationStatus.ACTIVE });
    }
    if (!finalEvent) return;

    if (!activation.finalEvent) {
      const status = finalEvent.eventType === 'ActivationCompleted'
        ? ActivationStatus.ACTIVE
        : ActivationStatus.FAILED;
      const updated = await this.activations.findOneAndUpdate(
        { activationId, finalEvent: null },
        {
          $set: { status, finalEvent },
          $push: { history: finalEvent },
        },
        { new: true },
      ).exec();
      if (!updated) {
        await this.publishFinalEvent(activationId, customerId);
        return;
      }
    }

    const latest = await this.activations.findOne({ activationId }).exec();
    if (!latest?.finalEvent || latest.finalEventPublished) return;
    await this.publishEvent(TOPICS.activationEvents, latest.finalEvent);
    await this.activations.updateOne(
      { activationId, finalEventPublished: false },
      { $set: { finalEventPublished: true } },
    ).exec();
    this.gateway.publish(latest.finalEvent);
  }

  private async publishEvent(topic: string, event: ActivationEvent): Promise<void> {
    await this.producer.send({
      topic,
      messages: [{
        key: event.customerId,
        value: JSON.stringify(event),
      }],
    });
  }
}