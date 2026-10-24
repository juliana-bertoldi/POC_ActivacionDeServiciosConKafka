import {
  DEAD_LETTER_TOPICS,
  isActivationEvent,
  TOPICS,
  type ActivationEvent,
  type TopicName,
} from '@activation-poc/contracts';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, type KafkaMessage } from 'kafkajs';
import { MongoClient, MongoServerError, type Collection, type Document } from 'mongodb';

interface LoyaltyEventRecord extends Document {
  event: ActivationEvent;
  topic: TopicName;
  partition: number;
  offset: string;
  recordedAt: Date;
}

const businessTopics = Object.values(TOPICS);
const retryDelaysMs = [1_000, 2_000, 4_000];

@Injectable()
export class LoyaltyService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(LoyaltyService.name);
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'loyalty-service',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((broker) => broker.trim()),
  });
  private readonly consumer = this.kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID ?? 'loyalty-service-replay',
  });
  private readonly producer = this.kafka.producer();
  private readonly mongo = new MongoClient(
    process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation_db?directConnection=true',
  );
  private eventLog?: Collection<LoyaltyEventRecord>;

  async onModuleInit(): Promise<void> {
    await Promise.all([
      this.consumer.connect(),
      this.producer.connect(),
      this.mongo.connect(),
    ]);

    this.eventLog = this.mongo.db().collection<LoyaltyEventRecord>('loyalty_event_log');
    await this.eventLog.createIndex({ 'event.eventId': 1 }, { unique: true });

    await Promise.all(businessTopics.map((topic) => this.consumer.subscribe({
      topic,
      fromBeginning: true,
    })));

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        await this.handleMessage(topic as TopicName, partition, message);
      },
    });

    this.logger.log(
      `Replay iniciado desde el principio: topics=${businessTopics.join(', ')}; groupId=${process.env.KAFKA_GROUP_ID ?? 'loyalty-service-replay'}`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled([
      this.consumer.disconnect(),
      this.producer.disconnect(),
      this.mongo.close(),
    ]);
  }

  private async handleMessage(topic: TopicName, partition: number, message: KafkaMessage): Promise<void> {
    let lastError: unknown;

    for (let attempt = 0; attempt <= retryDelaysMs.length; attempt += 1) {
      try {
        await this.recordMessage(topic, partition, message);
        await this.commitOffset(topic, partition, message);
        return;
      } catch (error) {
        lastError = error;
        if (attempt === retryDelaysMs.length) break;
        const reason = error instanceof Error ? error.message : String(error);
        this.logger.warn(`Intento ${attempt + 1} fallido para ${topic}[${partition}]@${message.offset}: ${reason}`);
        await new Promise((resolve) => setTimeout(resolve, retryDelaysMs[attempt]));
      }
    }

    const reason = lastError instanceof Error ? lastError.message : String(lastError);
    await this.producer.send({
      topic: DEAD_LETTER_TOPICS[topic],
      messages: [{
        key: message.key,
        value: message.value,
        headers: {
          'x-error': reason,
          'x-original-topic': topic,
          'x-original-partition': String(partition),
          'x-original-offset': message.offset,
        },
      }],
    });
    this.logger.error(`Evento enviado a ${DEAD_LETTER_TOPICS[topic]} luego de agotar reintentos: ${reason}`);
    await this.commitOffset(topic, partition, message);
  }

  private async recordMessage(topic: TopicName, partition: number, message: KafkaMessage): Promise<void> {
    if (!message.value) throw new Error(`Mensaje sin contenido en ${topic}[${partition}]@${message.offset}`);

    const parsed: unknown = JSON.parse(message.value.toString());
    if (!isActivationEvent(parsed)) {
      throw new Error(`Sobre de evento inválido en ${topic}[${partition}]@${message.offset}`);
    }

    const eventLog = this.eventLog;
    if (!eventLog) throw new Error('MongoDB todavía no está inicializado');

    try {
      await eventLog.insertOne({
        event: parsed,
        topic,
        partition,
        offset: message.offset,
        recordedAt: new Date(),
      });
      this.logger.log(`Replay ${parsed.eventType} eventId=${parsed.eventId} topic=${topic}`);
    } catch (error) {
      if (!(error instanceof MongoServerError) || error.code !== 11000) throw error;
      this.logger.debug(`Evento ya registrado en el replay: ${parsed.eventId}`);
    }
  }

  private async commitOffset(topic: TopicName, partition: number, message: KafkaMessage): Promise<void> {
    await this.consumer.commitOffsets([{
      topic,
      partition,
      offset: (BigInt(message.offset) + 1n).toString(),
    }]);
  }
}
