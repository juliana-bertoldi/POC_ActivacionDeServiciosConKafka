# Documentación de División de Tareas y Responsabilidades (POC Kafka)

> **Materia:** Sistemas Distribuidos  
> **Institución:** UTN FRCU — Facultad Regional Concepción del Uruguay  
> **Carrera:** Ingeniería en Sistemas de Información  
> **Proyecto:** POC Activación de Servicios con Kafka, NestJS y React[cite: 1]

---

## 📌 Visión General del Reparto

Para abordar la arquitectura orientada a eventos (EDA) propuesta en la POC, el desarrollo se divide en dos roles complementarios:

* **Persona A — Núcleo Distribuido & Servicios Transaccionales:** Se encarga de la infraestructura de contenedores, la librería de resiliencia compartida, la API Gateway con orquestación de la Saga y los microservicios con lógica de negocio y compensación (Billing y Provisioning)[cite: 1].
* **Persona B — Interfaz Web, Notificaciones & Observabilidad:** Se encarga de la interfaz interactiva en tiempo real (React + WebSockets), el consumo desacoplado de notificaciones (Mailhog), el análisis global de eventos (CRM Analytics) y la validación de contratos tipados[cite: 1].

---

## 🔹 Persona A: Núcleo y Lógica de Negocio Principal

### 1. Infraestructura Base y Configuración Inicial
* **`docker-compose.yml`:** Configuración de servicios de infraestructura base[cite: 1, 2]:
  * Apache Kafka (1 broker en modo KRaft, puerto `9092`)[cite: 2].
  * Kafka UI (`kafbat/kafka-ui` en puerto `8080`)[cite: 2].
  * MongoDB (Replica set de 1 nodo en puerto `27017` para habilitar transacciones y outbox)[cite: 2].
* **`docker/create-topics.sh`:** Script de creación automática de los 4 topics principales (`activation.requested`, `billing.events`, `provisioning.events`, `activation.events`) y sus correspondientes DLQ (`*.dlq`), todos configurados con 3 particiones y retención de 7 días[cite: 1, 2].
* **`libs/contracts` (Co-desarrollo inicial):** Definición del sobre común del evento (`eventId`, `eventType`, `correlationId`, `customerId`, etc.) y exportación de interfaces de TypeScript para cada Payload[cite: 1].

### 2. Módulo de Resiliencia (`libs/kafka-toolkit`)
* **Control de Idempotencia:** Implementación del guardado transaccional de `eventId` en la colección `processed_events` de MongoDB para evitar procesamiento de mensajes duplicados[cite: 1, 2].
* **Estrategia de Reintentos:** Lógica de reintento exponencial (3 intentos con demoras de 1s, 2s y 4s) ante fallos técnicos[cite: 1, 2].
* **Dead Letter Queue (DLQ):** Redireccionamiento automático de mensajes fallidos a `<topic>.dlq` tras agotar los 3 reintentos, incluyendo cabeceras con el motivo del error[cite: 1, 2].

### 3. Aplicaciones y Microservicios a Cargo
* **`apps/activation-api`:**
  * Endpoint REST `POST /activations` (responde `202 Accepted` con estado `PENDING`)[cite: 1].
  * Publicación de `ActivationRequested` en Kafka[cite: 1].
  * Agregador de la Saga: consumo de eventos de respuesta de `billing` y `provisioning`, evaluación de estado (`IN_PROGRESS`, `ACTIVE`, `FAILED`) y emisión final de `ActivationCompleted` o `ActivationFailed`[cite: 1].
  * `@WebSocketGateway` para emitir cambios de estado en vivo hacia la UI[cite: 1].
* **`apps/billing-service`:**
  * Consumo de `ActivationRequested` para creación simulada de cuentas en la colección `billing_accounts`[cite: 1].
  * Publicación de `BillingAccountCreated` o `BillingFailed`[cite: 1].
  * Lógica de compensación: consumo de `ActivationFailed` para anular la cuenta (`BillingAccountCancelled`)[cite: 1].
* **`apps/provisioning-service`:**
  * Consumo de `ActivationRequested` simulando retardo de aprovisionamiento (1 a 3 segundos)[cite: 1].
  * Simulación de fallos forzados según la propiedad `simulateFailure` del payload[cite: 1].
  * Publicación de `ProvisioningCompleted` o `ProvisioningFailed`[cite: 1].

---

## 🔹 Persona B: Frontend, Notificaciones y Analítica

### 1. Interfaz de Usuario Web (`apps/demo-ui`)
* **Tecnología:** React + Vite + Tailwind/CSS + `socket.io-client`[cite: 2].
* **Formulario de Contratación:** Selección de `customerId`, `planId` y dropdown de pruebas para simular fallos forzados (Ninguno, Billing, Provisioning)[cite: 1].
* **Timeline de Eventos en Tiempo Real:** Conexión vía WebSocket con `activation-api` para renderizar secuencialmente la línea de tiempo de cada activación rastreada por `correlationId`[cite: 1].
* **Consulta de Historial:** Vista para consultar el estado actual e historial de eventos consumiendo `GET /activations/:id`[cite: 1].

### 2. Servicios de Notificación e Integración Externa
* **`apps/notification-service`:**
  * Consumo desacoplado del topic `activation.events`[cite: 1, 2].
  * Filtro de eventos `ActivationCompleted` y `ActivationFailed`[cite: 1, 2].
  * Envío de correo electrónico de bienvenida o aviso de error integrando la API/SMTP del contenedor **Mailhog** (puerto `8025` / `1025`)[cite: 1, 2].

### 3. Registro Global y Trazabilidad (`apps/crm-analytics-service`)
* **`apps/crm-analytics-service`:**
  * Consumidor pasivo de **todos** los topics del sistema (`activation.requested`, `billing.events`, `provisioning.events`, `activation.events`)[cite: 1, 2].
  * Almacenamiento auditado del historial completo en la colección `event_log` de MongoDB[cite: 1].
  * Soporte para el **Escenario 5 de la demo (Replay)**: capacidad de procesar desde el inicio (`auto.offset.reset=earliest`) en un Consumer Group independiente sin interferir en el resto de los servicios[cite: 1].

### 4. Integración de Contratos (`libs/contracts`)
* Consumo estricto de los tipos de TypeScript compartidos para garantizar que la UI y los servicios de Notificación/CRM respeten los payloads sin romper contratos de compatibilidad[cite: 1].

---

## 📋 Matriz Resumen de Componentes y Responsables

| Componente / Característica | Ubicación en el Monorepo | Responsable |
| :--- | :--- | :---: |
| Docker Compose & Script de Topics | `docker/`, `docker-compose.yml` | **Persona A** |
| Contratos de Eventos TypeScript | `libs/contracts` | **Persona A / B** |
| Toolkit de Kafka (Idempotencia, DLQ) | `libs/kafka-toolkit` | **Persona A** |
| API REST + WebSockets + Orquestador Saga | `apps/activation-api` | **Persona A** |
| Servicio de Facturación & Compensación | `apps/billing-service` | **Persona A** |
| Servicio de Aprovisionamiento | `apps/provisioning-service` | **Persona A** |
| Interfaz de Usuario React (Timeline) | `apps/demo-ui` | **Persona B** |
| Servicio de Notificación (Emails / Mailhog) | `apps/notification-service` | **Persona B** |
| Servicio de Analítica / CRM (Replay) | `apps/crm-analytics-service` | **Persona B** |

---

## 🚀 Estrategia de Trabajo en Live Share y Git

1. **Sincronización Inicial:** Configurar juntos el archivo `libs/contracts/src/index.ts` antes de comenzar el código de las aplicaciones.
2. **Ambiente de Ejecución:** Persona A levanta la infraestructura de Docker (`docker compose up -d`) en su máquina local.
3. **Compartición de Puertos en Live Share:** Persona A comparte mediante VS Code Live Share los siguientes puertos para que Persona B pueda probar en vivo desde su equipo:
   * `3000` (Activation API / WebSockets)
   * `5173` (Demo UI React)
   * `8080` (Kafka UI)
   * `8025` (Mailhog Web UI)
4. **Política de Commits:** Commits individuales frecuentemente especificando la app trabajada (ej. `feat(billing): agrega consumidor de ActivationRequested`).