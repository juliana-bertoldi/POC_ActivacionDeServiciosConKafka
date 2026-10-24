import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { AppController } from './app.controller.js';
import { Activation, ActivationSchema, ProcessedEvent, ProcessedEventSchema } from './schemas/activation.schema.js';
import { KafkaService } from './kafka/kafka.service.js';
import { ActivationGateway } from './websocket/activation.gateway.js';

@Module({
  imports: [
    MongooseModule.forRoot(
      process.env.MONGO_URI ?? 'mongodb://localhost:27017/activation_db?directConnection=true',
    ),
    MongooseModule.forFeature([
      { name: Activation.name, schema: ActivationSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [AppController],
  providers: [KafkaService, ActivationGateway],
  exports: [KafkaService],
})
export class AppModule {}