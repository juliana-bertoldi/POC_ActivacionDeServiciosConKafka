import {
  BadRequestException,
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  Post,
} from '@nestjs/common';
import type { CreateActivationRequest, SimulateFailure } from '@activation-poc/contracts';
import { KafkaService } from './kafka/kafka.service.js';

interface ActivationRequestInput extends Omit<CreateActivationRequest, 'simulateFailure'> {
  simulateFailure: SimulateFailure | false;
}

function isCreateActivationRequest(value: unknown): value is ActivationRequestInput {
  if (typeof value !== 'object' || value === null) return false;
  const request = value as Record<string, unknown>;
  return typeof request.customerId === 'string' && request.customerId.trim().length > 0
    && typeof request.planId === 'string' && request.planId.trim().length > 0
    && typeof request.channel === 'string' && request.channel.trim().length > 0
    && (request.simulateFailure === false
      || request.simulateFailure === 'none'
      || request.simulateFailure === 'billing'
      || request.simulateFailure === 'provisioning');
}

@Controller('activations')
export class AppController {
  constructor(private readonly kafkaService: KafkaService) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  async requestActivation(@Body() body: unknown) {
    if (!isCreateActivationRequest(body)) {
      throw new BadRequestException(
        'customerId, planId, channel y simulateFailure válido son obligatorios',
      );
    }

    const activation = await this.kafkaService.createActivation({
      ...body,
      simulateFailure: body.simulateFailure === false ? 'none' : body.simulateFailure,
    });

    return {
      activationId: activation.correlationId,
      correlationId: activation.correlationId,
      customerId: activation.customerId,
      planId: activation.payload.planId,
      status: 'PENDING',
      events: [activation],
    };
  }

  @Get(':id')
  async getActivation(@Param('id') id: string) {
    const activation = await this.kafkaService.findActivation(id);
    if (!activation) throw new NotFoundException(`No existe la activación ${id}`);
    return activation;
  }
}