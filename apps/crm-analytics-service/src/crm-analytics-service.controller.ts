import { Controller, Get } from '@nestjs/common';
import { CrmAnalyticsServiceService } from './crm-analytics-service.service';

@Controller()
export class CrmAnalyticsServiceController {
  constructor(private readonly crmAnalyticsServiceService: CrmAnalyticsServiceService) {}

  @Get()
  getHello(): string {
    return this.crmAnalyticsServiceService.getHello();
  }
}
