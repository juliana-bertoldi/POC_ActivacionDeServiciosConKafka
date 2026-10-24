# POC: Activación de servicios con Kafka

**Materia:** Sistemas Distribuidos (2026)
**Institución:** UTN FRCU - Facultad Regional Concepción del Uruguay
**Carrera:** Ingeniería en Sistemas de Información

## Objetivo

La POC demuestra una activación de servicios de telecomunicaciones con arquitectura
orientada a eventos: la interfaz solicita un alta y los servicios reaccionan a eventos
Kafka, sin llamadas HTTP entre microservicios. El flujo incluye facturación,
aprovisionamiento, notificaciones y registro analítico.

El entorno simula los sistemas externos y corre localmente. No incluye integraciones
reales, autenticación, alta disponibilidad ni pruebas de carga.

## Componentes

- **Demo UI:** React y Vite; crea activaciones y muestra el timeline por WebSocket.
- **Core services:** API REST `POST /activations`, consulta `GET /activations/:id`,
  agregación del estado de la saga y emisión de actualizaciones en tiempo real.
- **Activation worker:** consume las solicitudes y coordina la publicación de los
  resultados de facturación y aprovisionamiento.
- **Billing y provisioning:** consumidores independientes que publican sus resultados;
  ante un fallo se emite la compensación de facturación.
- **Notification:** consume el resultado final y envía el correo de demostración a
  Mailhog.
- **CRM analytics:** registra eventos de negocio en MongoDB.
- **Loyalty (perfil opcional):** consumidor de demostración que reproduce el historial
  de los cuatro topics en un consumer group nuevo, sin interferir con los demás.
- **Kafka/Kafka UI/MongoDB/Mailhog:** infraestructura local para mensajería, inspección,
  persistencia y correo de prueba.

Los contratos versionados y la validación de eventos están en
[`libs/contracts`](./libs/contracts/). Los topics son `activation.requested`,
`billing.events`, `provisioning.events` y `activation.events`; cada uno tiene su DLQ.

## Requisitos previos

- Docker y Docker Compose.
- Node.js 20 y pnpm 9 si se quiere trabajar o ejecutar las pruebas desde el host.

## Ejecución completa

Desde la raíz del repositorio:

```sh
docker compose up --build
```

La primera ejecución construye las imágenes. Cuando estén listos los servicios, abrir:

- Demo: [http://localhost:5173](http://localhost:5173)
- Kafka UI: [http://localhost:8080](http://localhost:8080)
- Mailhog: [http://localhost:8025](http://localhost:8025)
- API: [http://localhost:3000](http://localhost:3000)

La UI permite crear una activación con cliente, plan y un fallo opcional. Una solicitud
aceptada devuelve `202 Accepted` sin esperar el procesamiento asíncrono.

Para detener y quitar los contenedores:

```sh
docker compose down
```

Para eliminar también el volumen local de Kafka (se pierde su historial):

```sh
docker compose down -v
```

## Guion de demo

El guion detallado con los pasos, qué observar y comandos de apoyo está en
[`docs/demo-script.md`](./docs/demo-script.md). Los cinco escenarios son: camino feliz,
fallo con compensación, servicio detenido y recuperación, escalado de consumidores y
replay del historial con un consumidor nuevo.

El quinto escenario se ejecuta en otra terminal mientras el entorno principal está
levantado:

```sh
docker compose --profile loyalty up --build -d loyalty-service
docker compose logs -f loyalty-service
```

El grupo `loyalty-service-replay` comienza desde el principio y registra los eventos
reproducidos en MongoDB (`loyalty_event_log`). Para repetir el replay desde cero, usar
un `KAFKA_GROUP_ID` nuevo:

```sh
LOYALTY_KAFKA_GROUP_ID=loyalty-service-replay-2 \
  docker compose --profile loyalty up --build -d --force-recreate loyalty-service
```

El perfil no se inicia en el arranque normal de la demo.

## Desarrollo y validación

Instalar dependencias y construir los contratos compartidos:

```sh
corepack pnpm install
corepack pnpm --filter @activation-poc/contracts build
```

Para comprobar contratos y frontend:

```sh
corepack pnpm --filter @activation-poc/contracts test
corepack pnpm --filter demo-ui build
corepack pnpm --filter demo-ui lint
```

La API expone `POST /activations` y `GET /activations/:id`; los detalles de su formato
y ejecución local están en [`apps/core-services/README.md`](./apps/core-services/README.md).
