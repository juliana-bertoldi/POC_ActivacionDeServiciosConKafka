import {
  DEAD_LETTER_TOPICS,
  TOPICS,
  type ActivationCompletedEvent,
  type ActivationEvent,
  type ActivationFailedEvent,
} from '@activation-poc/contracts';
import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Kafka, type KafkaMessage } from 'kafkajs';
import { MongoClient, type Collection, type Document } from 'mongodb';
import nodemailer, { type Transporter } from 'nodemailer';

type NotificationEvent = ActivationCompletedEvent | ActivationFailedEvent;
type NotificationState = 'PROCESSING' | 'SENT';

interface NotificationRecord extends Document {
  eventId: string;
  eventType: NotificationEvent['eventType'];
  correlationId: string;
  customerId: string;
  state: NotificationState;
  createdAt: Date;
  updatedAt: Date;
  sentAt?: Date;
  messageId?: string;
}

const retryDelaysMs = [1_000, 2_000, 4_000];

function isNotificationEvent(value: unknown): value is NotificationEvent {
  if (!value || typeof value !== 'object') return false;
  const event = value as Partial<ActivationEvent>;
  return typeof event.eventId === 'string'
    && typeof event.correlationId === 'string'
    && typeof event.customerId === 'string'
    && (event.eventType === 'ActivationCompleted' || event.eventType === 'ActivationFailed')
    && typeof event.payload === 'object'
    && event.payload !== null;
}

function escapeHtml(value: string): string {
  const replacements: Record<string, string> = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  };
  return value.replace(/[&<>"']/g, (character) => replacements[character] ?? character);
}

@Injectable()
export class NotificationServiceService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(NotificationServiceService.name);
  private readonly kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID ?? 'notification-service',
    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',').map((broker) => broker.trim()),
  });
  private readonly consumer = this.kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID ?? 'notification-service',
  });
  private readonly producer = this.kafka.producer();
  private readonly mongo = new MongoClient(
    process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation_db?directConnection=true',
  );
  private readonly mailer: Transporter = nodemailer.createTransport({
    host: process.env.MAILHOG_HOST ?? 'localhost',
    port: Number(process.env.MAILHOG_PORT ?? 1025),
    secure: false,
  });
  private notifications?: Collection<NotificationRecord>;

  async onModuleInit(): Promise<void> {
    await Promise.all([
      this.consumer.connect(),
      this.producer.connect(),
      this.mongo.connect(),
    ]);

    this.notifications = this.mongo.db().collection<NotificationRecord>('notifications');
    await this.notifications.createIndex({ eventId: 1 }, { unique: true });
    await this.consumer.subscribe({
      topic: TOPICS.activationEvents,
      fromBeginning: process.env.KAFKA_FROM_BEGINNING === 'true',
    });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        await this.handleMessage(topic, partition, message);
      },
    });

    this.logger.log(`Escuchando el topic ${TOPICS.activationEvents}`);
  }

  async onModuleDestroy(): Promise<void> {
    await Promise.allSettled([
      this.consumer.disconnect(),
      this.producer.disconnect(),
      this.mongo.close(),
    ]);
    this.mailer.close();
  }

  private async handleMessage(topic: string, partition: number, message: KafkaMessage): Promise<void> {
    try {
      if (!message.value) throw new Error('El mensaje Kafka no tiene value');
      const parsed: unknown = JSON.parse(message.value.toString());

      if (!isNotificationEvent(parsed)) {
        await this.commitOffset(topic, partition, message);
        return;
      }

      await this.deliverWithRetries(parsed);
      await this.commitOffset(topic, partition, message);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      this.logger.error(`No se pudo procesar el evento de Kafka: ${reason}`);
      await this.publishToDeadLetter(topic, partition, message, reason);
      await this.commitOffset(topic, partition, message);
    }
  }

  private async deliverWithRetries(event: NotificationEvent): Promise<void> {
    for (let attempt = 0; attempt <= retryDelaysMs.length; attempt += 1) {
      try {
        await this.sendNotification(event);
        return;
      } catch (error) {
        if (attempt === retryDelaysMs.length) throw error;
        const reason = error instanceof Error ? error.message : String(error);
        this.logger.warn(`Intento ${attempt + 1} fallido para ${event.eventId}: ${reason}`);
        await new Promise((resolve) => setTimeout(resolve, retryDelaysMs[attempt]));
      }
    }
  }

  private async sendNotification(event: NotificationEvent): Promise<void> {
    const notifications = this.notifications;
    if (!notifications) throw new Error('MongoDB todavía no está inicializado');

    const existing = await notifications.findOne({ eventId: event.eventId });
    if (existing?.state === 'SENT') {
      this.logger.debug(`Evento ya notificado: ${event.eventId}`);
      return;
    }

    const now = new Date();
    await notifications.updateOne(
      { eventId: event.eventId },
      {
        $set: {
          eventType: event.eventType,
          correlationId: event.correlationId,
          customerId: event.customerId,
          state: 'PROCESSING',
          updatedAt: now,
        },
        $setOnInsert: { createdAt: now },
      },
      { upsert: true },
    );

    const failed = event.eventType === 'ActivationFailed';
    const subject = failed
      ? `No pudimos activar tu servicio (${event.correlationId})`
      : `Tu servicio ya está activo (${event.correlationId})`;
    const detail = failed
      ? `El paso de ${escapeHtml(event.payload.failedStep)} no pudo completarse: ${escapeHtml(event.payload.reason)}`
      : 'La activación de tu servicio se completó correctamente.';
    const customerId = escapeHtml(event.customerId);
    const correlationId = escapeHtml(event.correlationId);
    const result = await this.mailer.sendMail({
      from: process.env.NOTIFICATION_FROM ?? 'notificaciones@activation-poc.local',
      to: process.env.NOTIFICATION_TO ?? 'cliente@demo.local',
      subject,
      text: `${detail}\nCliente: ${event.customerId}\nActivación: ${event.correlationId}`,
      html: `<h2>${failed ? 'Activación fallida' : '¡Bienvenido!'}</h2><p>${detail}</p><p>Cliente: ${customerId}</p><p>Activación: ${correlationId}</p>`,
      headers: {
        'X-Correlation-Id': event.correlationId,
        'X-Event-Id': event.eventId,
      },
    });

    await notifications.updateOne(
      { eventId: event.eventId },
      { $set: { state: 'SENT', sentAt: new Date(), messageId: result.messageId, updatedAt: new Date() } },
    );
    this.logger.log(`${event.eventType} enviado a Mailhog para ${event.correlationId}`);
  }

  private async publishToDeadLetter(
    topic: string,
    partition: number,
    message: KafkaMessage,
    reason: string,
  ): Promise<void> {
    await this.producer.send({
      topic: DEAD_LETTER_TOPICS[TOPICS.activationEvents],
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
    this.logger.error(`Mensaje enviado a ${DEAD_LETTER_TOPICS[TOPICS.activationEvents]}: ${reason}`);
  }

  private async commitOffset(topic: string, partition: number, message: KafkaMessage): Promise<void> {
    await this.consumer.commitOffsets([{
      topic,
      partition,
      offset: (BigInt(message.offset) + 1n).toString(),
    }]);
  }
}
