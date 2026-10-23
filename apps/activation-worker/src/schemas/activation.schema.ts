import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type ActivationDocument = HydratedDocument<Activation>;

@Schema({ timestamps: true })
export class Activation {
  @Prop({ required: true })
  eventId!: string;

  @Prop({ required: true })
  userId!: string;

  @Prop({ required: true })
  planId!: string;

  @Prop({ default: 'PENDING' })
  status!: string;
}

export const ActivationSchema = SchemaFactory.createForClass(Activation);