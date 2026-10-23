import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { ActivationConsumer } from './consumer/activation.consumer.js';
import { Activation, ActivationSchema } from './schemas/activation.schema.js';

@Module({
  imports: [
    MongooseModule.forRoot(process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/activation_db'),
    MongooseModule.forFeature([{ name: Activation.name, schema: ActivationSchema }]),
  ],
  providers: [ActivationConsumer],
})
export class AppModule {}