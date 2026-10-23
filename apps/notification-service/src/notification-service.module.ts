import { Module } from '@nestjs/common';
import { NotificationServiceService } from './notification-service.service.js';

@Module({
  providers: [NotificationServiceService],
})
export class NotificationServiceModule {}
