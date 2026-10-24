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

  @Prop({ type: String, required: true })
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

  @Prop({ type: [String], default: [] })
  processedEventIds!: string[];
}

export const ActivationSchema = SchemaFactory.createForClass(Activation);
ActivationSchema.index({ eventId: 1 }, { unique: true });