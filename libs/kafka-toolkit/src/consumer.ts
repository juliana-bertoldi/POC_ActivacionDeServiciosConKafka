import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

export interface KafkaConsumerConfig {
  clientId: string;
  brokers: string[];
  groupId: string;
}

export class AppKafkaConsumer {
  private kafka: Kafka;
  private consumer: Consumer;
  private isConnected = false;

  constructor(config: KafkaConsumerConfig) {
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers,
    });
    this.consumer = this.kafka.consumer({ groupId: config.groupId });
  }

  async connect(topics: string[]): Promise<void> {
    if (!this.isConnected) {
      await this.consumer.connect();
      for (const topic of topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      }
      this.isConnected = true;
      console.log(`Kafka Consumer connected and subscribed to topics: ${topics.join(', ')}`);
    }
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      await this.consumer.disconnect();
      this.isConnected = false;
      console.log('Kafka Consumer disconnected.');
    }
  }

  async run(handler: (topic: string, message: any, payload: EachMessagePayload) => Promise<void>): Promise<void> {
    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        const { topic, message } = payload;
        const valueString = message.value ? message.value.toString() : '{}';
        let parsedValue: any;

        try {
          parsedValue = JSON.parse(valueString);
        } catch {
          parsedValue = valueString;
        }

        await handler(topic, parsedValue, payload);
      },
    });
  }
}