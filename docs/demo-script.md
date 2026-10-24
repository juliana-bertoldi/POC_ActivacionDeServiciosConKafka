# Guion de demo

**Duración estimada:** 15 minutos.
**Preparación:** iniciar `docker compose up --build` y abrir la Demo UI, Kafka UI y
Mailhog. Para los escenarios de resiliencia, usar una segunda terminal.

## 1. Camino feliz: fan-out

1. En la UI, crear una activación sin fallos.
2. Verificar que la respuesta inicial es inmediata y que el timeline recibe los eventos
   en vivo.
3. Mostrar el resultado exitoso en la UI y el correo en Mailhog.
4. En Kafka UI, observar cómo el mismo evento de solicitud desencadena los procesos
   independientes.

**Concepto:** fan-out y desacoplamiento mediante eventos.

## 2. Fallo y compensación: saga

1. Crear una activación seleccionando **Forzar fallo de Provisioning**.
2. En la UI, esperar el estado fallido y revisar el paso y motivo.
3. Mostrar el evento de compensación de Billing y el correo de error en Mailhog.

**Concepto:** saga por coreografía y acción compensatoria.

## 3. Servicio detenido y recuperación: offsets

1. Detener notificaciones y crear tres activaciones exitosas:

   ```sh
   docker compose stop notification-service
   ```

2. En Kafka UI, mostrar el lag del consumer group de notificaciones.
3. Reiniciar el consumidor y verificar que procesa los pendientes y aparecen los
   correos:

   ```sh
   docker compose start notification-service
   ```

**Concepto:** persistencia de mensajes y recuperación a partir de offsets.

## 4. Escalado: consumer groups y particiones

1. Iniciar una segunda instancia de Billing:

   ```sh
   docker compose up --build -d --scale billing-service=2 billing-service
   ```

2. En Kafka UI, mostrar el consumer group de Billing y la asignación de particiones.

**Concepto:** escalado horizontal de consumidores dentro de un grupo.

## 5. Consumidor nuevo: replay del historial

1. Iniciar el consumidor Loyalty opcional:

   ```sh
   docker compose --profile loyalty up --build -d loyalty-service
   ```

2. Mostrar sus logs. Al usar el grupo nuevo `loyalty-service-replay`, reproduce desde
   el inicio los eventos retenidos en los cuatro topics, aunque se hayan publicado antes
   de iniciar Loyalty.
3. En Kafka UI, comparar el grupo nuevo con los grupos de los servicios existentes y
   confirmar que el replay no cambia sus offsets.
4. La copia de los eventos reproducidos queda en MongoDB, colección
   `loyalty_event_log`.

Para repetir esta prueba desde el inicio, configurar un `KAFKA_GROUP_ID` nuevo: Kafka
solo aplica `fromBeginning` a un grupo que todavía no tiene offsets. Por ejemplo:

```sh
LOYALTY_KAFKA_GROUP_ID=loyalty-service-replay-2 \
  docker compose --profile loyalty up --build -d --force-recreate loyalty-service
```

**Concepto:** replay del log y posibilidad de incorporar consumidores sin modificar los
productores ni interrumpir a los consumidores existentes.

## Cierre

Resumir la correlación de eventos por `correlationId`, el manejo de errores y la
independencia entre servicios. La ejecución completa depende del entorno local; se
recomienda grabar un video de respaldo por si falla la red durante la presentación.
