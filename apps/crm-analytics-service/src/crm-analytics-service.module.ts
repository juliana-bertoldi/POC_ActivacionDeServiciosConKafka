import { Module } from '@nestjs/common';
import { CrmAnalyticsServiceService } from './crm-analytics-service.service.js';

@Module({
  providers: [CrmAnalyticsServiceService],
})
export class CrmAnalyticsServiceModule {}
