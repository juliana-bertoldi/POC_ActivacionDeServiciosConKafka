import { Module } from '@nestjs/common';
import { CrmAnalyticsServiceController } from './crm-analytics-service.controller';
import { CrmAnalyticsServiceService } from './crm-analytics-service.service';

@Module({
  imports: [],
  controllers: [CrmAnalyticsServiceController],
  providers: [CrmAnalyticsServiceService],
})
export class CrmAnalyticsServiceModule {}
