import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  await app.init();
  console.log('Activation Worker service is running and listening for events...');
}
bootstrap();