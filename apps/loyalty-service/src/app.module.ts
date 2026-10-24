import { Module } from '@nestjs/common';
import { LoyaltyService } from './loyalty.service.js';

@Module({ providers: [LoyaltyService] })
export class AppModule {}
