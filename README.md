# POC: Activación de Servicios orientada a Eventos con Kafka, NestJS y React

> **Materia:** Sistemas Distribuidos (2026)  
> **Institución:** UTN FRCU - Facultad Regional Concepción del Uruguay  
> **Carrera:** Ingeniería en Sistemas de Información  

---

## 📌 Descripción del Proyecto

Esta Prueba de Concepto (POC) demuestra cómo resolver la activación de un plan de telecomunicaciones mediante **arquitectura orientada a eventos (EDA)** utilizando **Apache Kafka**, sin llamadas HTTP directas entre microservicios.

Un cliente contrata un plan desde una interfaz web en React; esto desencadena en paralelo los procesos de facturación, aprovisionamiento, notificación por e-mail y registro analítico, coordinados mediante eventos y el patrón **Saga por coreografía**.

### ✨ Características Principales
- **Arquitectura basada en eventos:** Desacoplamiento total entre servicios.
- **Resiliencia e Idempotencia:** Procesamiento de eventos sin duplicación usando `processed_events` en MongoDB.
- **Manejo de Errores:** Reintentos exponenciales, Dead Letter Queue (DLQ) y compensación automática en facturación ante fallos de aprovisionamiento.
- **Monitoreo en Tiempo Real:** Timeline de eventos en vivo mediante WebSockets.

---

## 🛠️ Stack Tecnológico

- **Mensajería:** Apache Kafka (Modo KRaft, 1 Broker)
- **Backend:** NestJS + `@nestjs/microservices` (Transporte Kafka)
- **Frontend:** React + Vite + `socket.io-client`
- **Base de Datos:** MongoDB (Replica Set de 1 nodo para soporte de transacciones/outbox)
- **Observabilidad & Herramientas:** Kafka UI (Kafbat), Mailhog (Servidor SMTP de prueba)
- **Entorno:** Docker & Docker Compose
- **Monorepo:** NestJS Workspaces / pnpm

---

## 🏗️ Arquitectura del Sistema
