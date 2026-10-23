import { Test, TestingModule } from '@nestjs/testing';
import { CrmAnalyticsServiceController } from './crm-analytics-service.controller';
import { CrmAnalyticsServiceService } from './crm-analytics-service.service';

describe('CrmAnalyticsServiceController', () => {
  let crmAnalyticsServiceController: CrmAnalyticsServiceController;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [CrmAnalyticsServiceController],
      providers: [CrmAnalyticsServiceService],
    }).compile();

    crmAnalyticsServiceController = app.get<CrmAnalyticsServiceController>(CrmAnalyticsServiceController);
  });

  describe('root', () => {
    it('should return "Hello World!"', () => {
      expect(crmAnalyticsServiceController.getHello()).toBe('Hello World!');
    });
  });
});
