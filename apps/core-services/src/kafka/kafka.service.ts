import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { AppKafkaProducer } from '@activation-poc/kafka-toolkit';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private producer!: AppKafkaProducer;

  async onModuleInit() {
    this.producer = new AppKafkaProducer({
      clientId: 'core-service',
      brokers: [process.env.KAFKA_BROKER || '127.0.0.1:9092'],
    });
    await this.producer.connect();
    console.log('Core Service connected to Kafka Producer');
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }

  async sendActivationRequested(data: { eventId: string; userId: string; planId: string }) {
    await this.producer.send('activation.requested', {
      ...data,
      timestamp: new Date().toISOString(),
    });
    console.log(`Event sent: activation.requested for user ${data.userId}`);
  }
}