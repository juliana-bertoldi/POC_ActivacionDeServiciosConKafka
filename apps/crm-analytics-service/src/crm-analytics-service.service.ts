import { isActivationEvent, TOPICS } from '@activation-poc/contracts';
import type { ActivationEvent } from '@activation-poc/contracts';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, type KafkaMessage } from 'kafkajs';
import { MongoClient, MongoServerError, type Collection, type Document } from 'mongodb';

type BusinessTopic = (typeof TOPICS)[keyof typeof TOPICS];

interface EventLogRecord extends Document {
  eventId: string;
  eventType: string;
  correlationId: string;
  customerId: string;
  version: ActivationEvent['version'];
  occurredAt: string;
  source: string;
  payload: ActivationEvent['payload'];
  topic: BusinessTopic;
  partition: number;
  offset: string;
  recordedAt: Date;
}

const businessTopics = Object.values(TOPICS);

@Injectable()
export class CrmAnalyticsServiceService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(CrmAnalyticsServiceService.name);
  private readonly fromBeginning = process.env.KAFKA_FROM_BEGINNING === 'true';
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'crm-analytics-service',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((broker) => broker.trim()),
  });
  private readonly consumer = this.kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID ?? 'crm-analytics-service',
  });
  private readonly mongo = new MongoClient(
    process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation_db?directConnection=true',
  );
  private eventLog?: Collection<EventLogRecord>;

  async onModuleInit(): Promise<void> {
    await Promise.all([this.consumer.connect(), this.mongo.connect()]);

    this.eventLog = this.mongo.db().collection<EventLogRecord>('event_log');
    await Promise.all([
      this.eventLog.createIndex({ eventId: 1 }, { unique: true }),
      this.eventLog.createIndex({ correlationId: 1, occurredAt: 1 }),
      this.eventLog.createIndex({ topic: 1, recordedAt: 1 }),
    ]);

    await Promise.all(businessTopics.map((topic) => this.consumer.subscribe({
      topic,
      fromBeginning: this.fromBeginning,
    })));

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        await this.recordMessage(topic as BusinessTopic, partition, message);
      },
    });

    this.logger.log(
      `Escuchando ${businessTopics.join(', ')} con groupId=${process.env.KAFKA_GROUP_ID ?? 'crm-analytics-service'}; fromBeginning=${this.fromBeginning}`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled([this.consumer.disconnect(), this.mongo.close()]);
  }

  private async recordMessage(topic: BusinessTopic, partition: number, message: KafkaMessage): Promise<void> {
    if (!message.value) throw new Error(`Mensaje sin contenido en ${topic}[${partition}]@${message.offset}`);

    const parsed: unknown = JSON.parse(message.value.toString());
    if (!isActivationEvent(parsed)) {
      throw new Error(`Sobre de evento inválido en ${topic}[${partition}]@${message.offset}`);
    }

    const eventLog = this.eventLog;
    if (!eventLog) throw new Error('MongoDB todavía no está inicializado');

    const record: EventLogRecord = {
      ...parsed,
      topic,
      partition,
      offset: message.offset,
      recordedAt: new Date(),
    };

    try {
      await eventLog.insertOne(record);
      this.logger.log(`Registrado ${parsed.eventType} eventId=${parsed.eventId} topic=${topic}`);
    } catch (error) {
      if (!(error instanceof MongoServerError) || error.code !== 11000) throw error;
      this.logger.debug(`Evento duplicado ignorado: ${parsed.eventId}`);
    }

    await this.consumer.commitOffsets([{
      topic,
      partition,
      offset: (BigInt(message.offset) + 1n).toString(),
    }]);
  }
}
