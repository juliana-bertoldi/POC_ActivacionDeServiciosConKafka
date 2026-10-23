# POC: Activación de Servicios orientada a Eventos con Kafka, NestJS y React

> **Materia:** Sistemas Distribuidos (2026)  
> **Institución:** UTN FRCU - Facultad Regional Concepción del Uruguay  
> **Carrera:** Ingeniería en Sistemas de Información  

---

## 📌 Descripción del Proyecto

Esta Prueba de Concepto (POC) demuestra cómo resolver la activación de un plan de telecomunicaciones mediante **arquitectura orientada a eventos (EDA)** utilizando **Apache Kafka**, sin llamadas HTTP directas entre microservicios[cite: 1].

Un cliente contrata un plan desde una interfaz web en React; esto desencadena en paralelo los procesos de facturación, aprovisionamiento, notificación por e-mail y registro analítico, coordinados mediante eventos y el patrón **Saga por coreografía**[cite: 1].

### ✨ Características Principales
- **Arquitectura basada en eventos:** Desacoplamiento total entre servicios[cite: 1].
- **Resiliencia e Idempotencia:** Procesamiento de eventos sin duplicación usando `processed_events` en MongoDB[cite: 1].
- **Manejo de Errores:** Reintentos exponenciales, Dead Letter Queue (DLQ) y compensación automática en facturación ante fallos de aprovisionamiento[cite: 1].
- **Monitoreo en Tiempo Real:** Timeline de eventos en vivo mediante WebSockets[cite: 1].

---

## 🛠️ Stack Tecnológico

- **Mensajería:** Apache Kafka (Modo KRaft, 1 Broker)[cite: 2]
- **Backend:** NestJS + `@nestjs/microservices` (Transporte Kafka)[cite: 2]
- **Frontend:** React + Vite + `socket.io-client`[cite: 2]
- **Base de Datos:** MongoDB (Replica Set de 1 nodo para soporte de transacciones/outbox)[cite: 2]
- **Observabilidad & Herramientas:** Kafka UI (Kafbat), Mailhog (Servidor SMTP de prueba)[cite: 2]
- **Entorno:** Docker & Docker Compose[cite: 2]
- **Monorepo:** NestJS Workspaces / pnpm[cite: 2]

---

## 🏗️ Arquitectura del Sistema

```text
                        +------------------+
                        |     Demo UI      |
                        |  (React + Vite)  |
                        +--------+---------+
                                 |  ^
                POST /activations|  | WebSockets (Timeline)
                                 v  |
                      +----------+----------+
                      |    activation-api   |
                      |      (NestJS)       |
                      +----------+----------+
                                 |
                     Publica ActivationRequested
                                 v
   +-----------------------------+-----------------------------+
   |                        APACHE KAFKA                       |
   +-------+---------------------+---------------------+-------+
           |                     |                     |
           v                     v                     v
+----------+----------+ +--------+----------+ +--------+----------+
|   billing-service   | |provisioning-svc   | | crm-analytics-svc |
|       (NestJS)      | |     (NestJS)      | |      (NestJS)     |
+----------+----------+ +--------+----------+ +-------------------+
           |                     |
   BillingAccountCreated  ProvisioningCompleted
           |                     |
           +----------+----------+
                      |
                      v (Consume resultados y evalúa Saga)
              activation-api
                      |
           Publica ActivationCompleted / Failed
                      v
            +---------+---------+
            | notification-svc  | ---> Mailhog (Email)
            +-------------------+
