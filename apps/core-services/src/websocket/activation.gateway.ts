import type { ActivationEvent } from '@activation-poc/contracts';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import type { Server, Socket } from 'socket.io';

@WebSocketGateway({ cors: { origin: true } })
export class ActivationGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer()
  private server!: Server;

  private readonly logger = new Logger(ActivationGateway.name);

  handleConnection(client: Socket): void {
    this.logger.debug(`WebSocket client connected: ${client.id}`);
  }

  handleDisconnect(client: Socket): void {
    this.logger.debug(`WebSocket client disconnected: ${client.id}`);
  }

  publish(event: ActivationEvent): void {
    this.server.emit('activation.event', event);
  }
}
