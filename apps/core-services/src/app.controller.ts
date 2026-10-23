import { Controller, Post, Body } from '@nestjs/common';
import { KafkaService } from './kafka/kafka.service.js';

@Controller('activation')
export class AppController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Post()
  async requestActivation(@Body() body: { userId: string; planId: string }) {
    const eventId = `evt-${Date.now()}`;
    
    await this.kafkaService.sendActivationRequested({
      eventId,
      userId: body.userId || 'user-default-123',
      planId: body.planId || 'plan-standard',
    });

    return {
      success: true,
      message: 'Activation requested event published to Kafka!',
      eventId,
    };
  }
}