import {
  ActivationStatus,
  type ActivationEvent,
  type SimulateFailure,
} from '@activation-poc/contracts';
import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type ActivationDocument = HydratedDocument<Activation>;

@Schema({ timestamps: true })
export class Activation {
  @Prop({ type: String, required: true, unique: true })
  activationId!: string;

  @Prop({ type: String, required: true, unique: true })
  eventId!: string;

  @Prop({ type: String, required: true })
  customerId!: string;

  @Prop({ type: String, required: true })
  planId!: string;

  @Prop({ type: String, required: true })
  channel!: string;

  @Prop({ type: String, enum: ['none', 'billing', 'provisioning'], required: true })
  simulateFailure!: SimulateFailure;

  @Prop({ type: String, enum: Object.values(ActivationStatus), default: ActivationStatus.PENDING })
  status!: ActivationStatus;

  @Prop({ type: [Object], default: [] })
  history!: ActivationEvent[];

  @Prop({
    type: {
      billing: { type: String, enum: ['OK', 'FAILED', null], default: null },
      provisioning: { type: String, enum: ['OK', 'FAILED', null], default: null },
    },
    default: { billing: null, provisioning: null },
  })
  results!: { billing: 'OK' | 'FAILED' | null; provisioning: 'OK' | 'FAILED' | null };

  @Prop({ type: Object, default: null })
  finalEvent!: ActivationEvent | null;

  @Prop({ type: Boolean, default: false })
  finalEventPublished!: boolean;
}

export const ActivationSchema = SchemaFactory.createForClass(Activation);
ActivationSchema.index({ customerId: 1, createdAt: -1 });

@Schema({ collection: 'processed_events', timestamps: { createdAt: true, updatedAt: false } })
export class ProcessedEvent {
  @Prop({ type: String, required: true, unique: true })
  eventId!: string;
}

export type ProcessedEventDocument = HydratedDocument<ProcessedEvent>;
export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);
