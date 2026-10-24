import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableCors();
  app.enableShutdownHooks();
  const port = process.env.PORT || 3000;
  await app.listen(port);
  console.log(`Core Service is running on port ${port}`);
}
bootstrap();