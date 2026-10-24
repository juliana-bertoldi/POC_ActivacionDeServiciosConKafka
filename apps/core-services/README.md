# Core services: producer de activaciones

Este servicio expone `POST /activations` y publica un evento `ActivationRequested`
con el sobre versionado de `@activation-poc/contracts`. La clave Kafka es
`customerId`, para conservar el orden de los eventos de un cliente.

Con Kafka iniciado y accesible desde el host:

```sh
pnpm --filter @activation-poc/contracts build
pnpm --filter @activation-poc/core-services start:dev
```

La configuración acepta `KAFKA_BROKERS` (lista separada por comas) y, por
compatibilidad, `KAFKA_BROKER`; por defecto se conecta a `localhost:9092`.

Ejemplo:

```sh
curl -i -X POST http://localhost:3000/activations \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"C-1234","planId":"FIBRA-300","simulateFailure":"none","channel":"web"}'
```

La API responde `202 Accepted` con el `activationId`, el estado inicial
`PENDING` y el evento publicado. También se puede consultar `GET
/activations/:id` para obtener el estado e historial.

El `activation-worker` consume `activation.requested`, persiste idempotentemente
la solicitud, y publica resultados en paralelo en `billing.events` y
`provisioning.events`. `core-services` agrega ambos resultados, persiste los
cambios y publica `ActivationCompleted` o `ActivationFailed` en
`activation.events`. Ante un fallo, el worker recibe el evento de cierre y
publica la compensación de facturación. El dashboard recibe todos estos eventos
por Socket.IO en tiempo real. Los mensajes que siguen fallando tras los
reintentos se publican en el topic DLQ correspondiente.
