import 'dotenv/config';
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { CrmAnalyticsServiceModule } from './crm-analytics-service.module.js';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(CrmAnalyticsServiceModule, {
    bufferLogs: true,
  });
  app.enableShutdownHooks();
}

void bootstrap();
