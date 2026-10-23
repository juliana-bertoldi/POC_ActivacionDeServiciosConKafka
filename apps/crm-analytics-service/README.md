# CRM Analytics Service

Consumidor pasivo de los cuatro topics de negocio. Persiste el sobre completo de cada evento en la colección MongoDB `event_log`; no publica eventos ni modifica los flujos de otros servicios.

## Desarrollo local

Se necesitan Kafka y MongoDB disponibles. Copiar `.env.example` a `.env` y ejecutar desde la raíz:

```sh
corepack pnpm install
corepack pnpm --config.engine-strict=false --filter @activation-poc/contracts build
corepack pnpm --config.engine-strict=false --filter crm-analytics-service start:dev
```

El consumidor confirma el offset después de guardar el evento. Un índice único por `eventId` evita duplicados y el evento duplicado se confirma sin volver a insertarlo.

## Replay

Para releer el historial, detener la instancia normal y usar un grupo nuevo e independiente:

```env
KAFKA_GROUP_ID=crm-analytics-replay-20261023
KAFKA_FROM_BEGINNING=true
```

Kafka aplica `fromBeginning` solo cuando ese grupo no tiene offsets previos. El grupo nuevo evita interferir con el consumer group normal ni con los demás servicios.

Dentro de Docker usar `KAFKA_BROKERS=kafka:29092` y `MONGO_URI=mongodb://mongo:27017/activation_db?replicaSet=rs0`; desde el host se usan los valores de `.env.example`.

## Prueba manual sin activation-api

Levantar la infraestructura y el worker, y publicar un mensaje de prueba en uno de los topics:

```sh
docker compose up -d kafka kafka-init mongo
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:29092 --topic activation.requested <<'EOF'
{"eventId":"crm-demo-001","eventType":"ActivationRequested","version":1,"occurredAt":"2026-10-23T12:00:00.000Z","correlationId":"act-crm-demo-001","customerId":"C-1234","source":"manual-test","payload":{"planId":"FIBRA-300","channel":"web","simulateFailure":"none"}}
EOF
```

El worker debe registrar `ActivationRequested` en sus logs. Para inspeccionar Mongo:

```sh
docker compose exec mongo mongosh --quiet --eval 'db.getSiblingDB("activation_db").event_log.find({eventId:"crm-demo-001"}).forEach(printjson)'
```

El documento conserva el sobre original y agrega `topic`, `partition`, `offset` y `recordedAt`. Se puede repetir el mismo mensaje: el `eventId` único evita insertar un segundo registro.