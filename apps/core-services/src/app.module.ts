import { Module } from '@nestjs/common';
import { AppController } from './app.controller.js';
import { KafkaService } from './kafka/kafka.service.js';

@Module({
  imports: [],
  controllers: [AppController],
  providers: [KafkaService],
  exports: [KafkaService],
})
export class AppModule {}