# Notification Service

Worker NestJS que consume `activation.events`, envía un correo por cada `ActivationCompleted` o `ActivationFailed` y no publica eventos de negocio.

## Desarrollo local

Se necesitan Kafka, MongoDB y Mailhog disponibles. Copiar `.env.example` a `.env` para ejecutar el worker en el host y luego, desde la raíz del monorepo:

```sh
corepack pnpm install
corepack pnpm --config.engine-strict=false --filter @activation-poc/contracts build
corepack pnpm --config.engine-strict=false --filter notification-service start:dev
```

El worker usa el grupo `notification-service`, mantiene `KAFKA_FROM_BEGINNING=false` por defecto y guarda el `eventId`/estado de envío en la colección `notifications` de Mongo para evitar correos repetidos al reprocesar un mensaje enviado.

Si se ejecuta dentro de Docker, usar `KAFKA_BROKERS=kafka:29092`, `MONGO_URI=mongodb://mongo:27017/activation_db?replicaSet=rs0` y `MAILHOG_HOST=mailhog`. Desde el host, los valores de `.env.example` usan `localhost`.

## Verificación

Una activación exitosa produce un correo de bienvenida; una fallida produce un aviso indicando el paso y el motivo. Mailhog muestra los correos en [http://localhost:8025](http://localhost:8025). Si el envío falla tras cuatro intentos (inicial más tres reintentos), el mensaje se publica en `activation.events.dlq` con cabeceras del error y origen. Kafka UI está en `http://localhost:8080`.

El sobre actual de eventos no contiene email del cliente, por lo que el destinatario se configura con `NOTIFICATION_TO` (por defecto `cliente@demo.local`).