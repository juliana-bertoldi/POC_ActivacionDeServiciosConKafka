import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { Kafka, Consumer } from 'kafkajs';
import { Activation, ActivationDocument } from '../schemas/activation.schema.js';

@Injectable()
export class ActivationConsumer implements OnModuleInit, OnModuleDestroy {
  private consumer: Consumer;

  constructor(
    @InjectModel(Activation.name) private activationModel: Model<ActivationDocument>,
  ) {
    const kafka = new Kafka({
      clientId: 'activation-worker',
      brokers: [process.env.KAFKA_BROKER || '127.0.0.1:9092'],
    });
    this.consumer = kafka.consumer({ groupId: 'activation-group' });
  }

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: 'activation.requested', fromBeginning: true });

    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        if (!message.value) return;
        const eventData = JSON.parse(message.value.toString());
        
        console.log(`[Worker] Received event from topic "${topic}":`, eventData);
        
        try {
          // Guardar en MongoDB
          const createdActivation = await this.activationModel.create({
            eventId: eventData.eventId || 'unknown',
            userId: eventData.userId,
            planId: eventData.planId,
            status: 'COMPLETED',
          });

          console.log(`[Worker] Successfully saved activation to MongoDB with ID: ${createdActivation._id}`);
        } catch (error) {
          console.error(`[Worker] Error saving activation to MongoDB:`, error);
        }
      },
    });

    console.log('Activation Worker successfully subscribed to Kafka topic "activation.requested"');
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }
}