import { Kafka, Producer } from 'kafkajs';

export interface KafkaProducerConfig {
  clientId: string;
  brokers: string[];
}

export class AppKafkaProducer {
  private kafka: Kafka;
  private producer: Producer;
  private isConnected = false;

  constructor(config: KafkaProducerConfig) {
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers,
    });
    this.producer = this.kafka.producer();
  }

  async connect(): Promise<void> {
    if (!this.isConnected) {
      await this.producer.connect();
      this.isConnected = true;
      console.log('Kafka Producer connected successfully.');
    }
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      await this.producer.disconnect();
      this.isConnected = false;
      console.log('Kafka Producer disconnected.');
    }
  }

  async send(topic: string, message: object, key?: string): Promise<void> {
    if (!this.isConnected) {
      await this.connect();
    }

    await this.producer.send({
      topic,
      messages: [
        {
          key: key ? key : null,
          value: JSON.stringify(message),
          timestamp: Date.now().toString(),
        },
      ],
    });
  }
}