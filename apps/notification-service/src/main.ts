import 'dotenv/config';
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { NotificationServiceModule } from './notification-service.module.js';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(NotificationServiceModule, {
    bufferLogs: true,
  });
  app.enableShutdownHooks();
}

void bootstrap();
