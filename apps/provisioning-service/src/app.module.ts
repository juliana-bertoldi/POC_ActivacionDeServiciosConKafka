import { Module } from '@nestjs/common';
import { ProvisioningService } from './provisioning.service.js';

@Module({ providers: [ProvisioningService] })
export class AppModule {}
