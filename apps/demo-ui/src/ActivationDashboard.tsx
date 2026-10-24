import {
  ActivationStatus,
  isActivationEvent,
  type ActivationEvent,
  type CreateActivationRequest,
  type SimulateFailure,
} from '@activation-poc/contracts'
import { useEffect, useState, type FormEvent } from 'react'
import { io } from 'socket.io-client'
import './ActivationDashboard.css'

type Screen = 'activation' | 'history'
type SocketState = 'connecting' | 'connected' | 'disconnected'

interface ActivationRecord {
  activationId: string
  correlationId: string
  customerId: string
  planId: string
  status: ActivationStatus
  events: ActivationEvent[]
}

interface ApiActivation extends Partial<ActivationRecord> {
  id?: string
  history?: ActivationEvent[]
  activation?: ApiActivation
}

const apiUrl = import.meta.env.VITE_ACTIVATION_API_URL ?? 'http://localhost:3000'
const socketUrl = import.meta.env.VITE_ACTIVATION_SOCKET_URL ?? apiUrl
function getStatus(event: ActivationEvent, current: ActivationStatus): ActivationStatus {
  if (event.eventType === 'ActivationCompleted') return ActivationStatus.ACTIVE
  if (event.eventType === 'ActivationFailed') return ActivationStatus.FAILED
  if (event.eventType === 'ActivationRequested') return ActivationStatus.IN_PROGRESS
  return current
}

function appendEvent(records: ActivationRecord[], event: ActivationEvent): ActivationRecord[] {
  const current = records.find((record) => record.correlationId === event.correlationId)
  if (current?.events.some((item) => item.eventId === event.eventId)) return records
  if (current) return records.map((record) => record.correlationId === event.correlationId
    ? { ...record, events: [...record.events, event], status: getStatus(event, record.status) }
    : record)
  return [{
    activationId: event.correlationId,
    correlationId: event.correlationId,
    customerId: event.customerId,
    planId: '',
    status: getStatus(event, ActivationStatus.PENDING),
    events: [event],
  }, ...records]
}

function toActivation(data: ApiActivation, fallbackId: string): ActivationRecord {
  const result = data.activation ?? data
  const events = (result.events ?? result.history ?? []).filter(isActivationEvent)
  return {
    activationId: result.activationId ?? result.id ?? fallbackId,
    correlationId: result.correlationId ?? events[0]?.correlationId ?? fallbackId,
    customerId: result.customerId ?? events[0]?.customerId ?? '',
    planId: result.planId ?? '',
    status: result.status ?? ActivationStatus.PENDING,
    events,
  }
}

function eventLabel(event: ActivationEvent) {
  switch (event.eventType) {
    case 'ActivationRequested': return 'Solicitud recibida'
    case 'BillingAccountCreated': return 'Cuenta de facturación creada'
    case 'BillingFailed': return 'Falló la facturación'
    case 'BillingAccountCancelled': return 'Cuenta de facturación compensada'
    case 'ProvisioningCompleted': return 'Aprovisionamiento completado'
    case 'ProvisioningFailed': return 'Falló el aprovisionamiento'
    case 'ActivationCompleted': return 'Servicio activado'
    case 'ActivationFailed': return 'Activación fallida'
  }
}

function App() {
  const [screen, setScreen] = useState<Screen>('activation')
  const [customerId, setCustomerId] = useState('CLI-001')
  const [planId, setPlanId] = useState('FIBRA-300')
  const [simulateFailure, setSimulateFailure] = useState<SimulateFailure>('none')
  const [activations, setActivations] = useState<ActivationRecord[]>([])
  const [selectedId, setSelectedId] = useState('')
  const [historyId, setHistoryId] = useState('')
  const [socketState, setSocketState] = useState<SocketState>('connecting')
  const [busy, setBusy] = useState(false)
  const [notice, setNotice] = useState('')

  useEffect(() => {
    const socket = io(socketUrl, { transports: ['websocket', 'polling'] })
    socket.on('connect', () => setSocketState('connected'))
    socket.on('disconnect', () => setSocketState('disconnected'))
    socket.on('connect_error', () => setSocketState('disconnected'))
    socket.onAny((_name, payload: unknown) => {
      if (!isActivationEvent(payload)) return
      setActivations((current) => appendEvent(current, payload))
      setSelectedId((current) => current || payload.correlationId)
    })
    return () => {
      socket.disconnect()
    }
  }, [])

  const selected = activations.find((item) => item.activationId === selectedId || item.correlationId === selectedId)

  async function submitActivation(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setBusy(true)
    setNotice('')
    try {
      const request: CreateActivationRequest = { customerId, planId, simulateFailure, channel: 'web' }
      const response = await fetch(`${apiUrl}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request),
      })
      if (!response.ok) throw new Error(`La API respondió ${response.status}`)
      const data = await response.json() as ApiActivation
      const created = toActivation(data, data.activationId ?? data.id ?? crypto.randomUUID())
      setActivations((current) => {
        const existing = current.find((item) => item.correlationId === created.correlationId)
        const merged = existing ? { ...created, events: existing.events } : created
        return [merged, ...current.filter((item) => item.correlationId !== created.correlationId)]
      })
      setSelectedId(created.correlationId)
      setScreen('activation')
      setNotice('Solicitud enviada. Siguiendo los eventos de esta activación.')
    } catch (error) {
      setNotice(error instanceof Error ? error.message : 'No se pudo iniciar la activación.')
    } finally {
      setBusy(false)
    }
  }

  async function searchHistory(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const id = historyId.trim()
    if (!id) return
    setBusy(true)
    setNotice('')
    try {
      const response = await fetch(`${apiUrl}/activations/${encodeURIComponent(id)}`)
      if (!response.ok) throw new Error(response.status === 404
        ? 'No encontramos una activación con ese identificador.'
        : `La API respondió ${response.status}`)
      const activation = toActivation(await response.json() as ApiActivation, id)
      setActivations((current) => [activation, ...current.filter((item) => item.correlationId !== activation.correlationId)])
      setSelectedId(activation.correlationId)
      setScreen('history')
    } catch (error) {
      setNotice(error instanceof Error ? error.message : 'No se pudo consultar el historial.')
    } finally {
      setBusy(false)
    }
  }

  return <div className="app-shell">
    <aside className="rail">
      <a className="brand-mark" href="#inicio" aria-label="Activaciones">A<span>.</span></a>
      <span className="rail-label">OPERACIONES</span>
      <button className={`rail-link ${screen === 'activation' ? 'is-active' : ''}`} onClick={() => setScreen('activation')}><span className="rail-number">01</span><span>Activar</span></button>
      <button className={`rail-link ${screen === 'history' ? 'is-active' : ''}`} onClick={() => setScreen('history')}><span className="rail-number">02</span><span>Historial</span></button>
      <div className="rail-bottom">POC<br />KAFKA</div>
    </aside>

    <main className="workspace">
      <header className="topbar"><div className="breadcrumb"><span>PLATAFORMA</span><span>/</span><strong>ACTIVACIONES</strong></div><div className={`connection connection--${socketState}`}><span className="connection-dot" />{socketState === 'connected' ? 'En vivo' : socketState === 'connecting' ? 'Conectando' : 'Sin conexión'}</div></header>
      <section className="page-heading"><div><p className="eyebrow">CENTRO DE OPERACIONES <span>·</span> 01</p><h1>{screen === 'activation' ? 'Activación de servicios' : 'Historial de activaciones'}</h1><p className="page-description">{screen === 'activation' ? 'Iniciá una solicitud y seguí cada paso de la saga en tiempo real.' : 'Consultá el estado actual y la secuencia completa de eventos.'}</p></div><div className="heading-index"><span>EVENT-DRIVEN</span><strong>v1.0</strong></div></section>
      {notice && <div className="notice" role="status">{notice}<button aria-label="Cerrar aviso" onClick={() => setNotice('')}>×</button></div>}

      <section className={`content-grid ${screen === 'history' ? 'content-grid--history' : ''}`}>
        {screen === 'activation' && <section className="panel request-panel">
          <div className="panel-heading"><div><span className="section-number">01 / NUEVA SOLICITUD</span><h2>Contratar un plan</h2></div><span className="panel-symbol">+</span></div>
          <form onSubmit={submitActivation}>
            <label htmlFor="customer-id">Identificador de cliente</label><input id="customer-id" value={customerId} onChange={(event) => setCustomerId(event.target.value)} placeholder="Ej. CLI-1042" required />
            <label htmlFor="plan-id">Plan de servicio</label><select id="plan-id" value={planId} onChange={(event) => setPlanId(event.target.value)}><option value="FIBRA-300">Fibra 300 MB</option><option value="FIBRA-600">Fibra 600 MB</option><option value="MOVIL-20">Móvil 20 GB</option></select>
            <label htmlFor="simulate-failure">Simulación de prueba <span className="optional">OPCIONAL</span></label><select id="simulate-failure" value={simulateFailure} onChange={(event) => setSimulateFailure(event.target.value as SimulateFailure)}><option value="none">Sin fallos</option><option value="billing">Forzar fallo de Billing</option><option value="provisioning">Forzar fallo de Provisioning</option></select>
            <div className="form-footnote"><span className="footnote-mark">i</span><span>La prueba recorre el mismo flujo distribuido que una contratación real.</span></div>
            <button className="primary-button" type="submit" disabled={busy}><span>{busy ? 'Enviando solicitud…' : 'Iniciar activación'}</span><span aria-hidden="true">↗</span></button>
          </form>
        </section>}

        <section className="panel timeline-panel">
          <div className="panel-heading timeline-heading"><div><span className="section-number">{screen === 'history' ? '02 / CONSULTA' : '02 / SEGUIMIENTO'}</span><h2>{screen === 'history' ? 'Buscar activación' : 'Actividad reciente'}</h2></div><span className={`live-label ${socketState === 'connected' ? 'live-label--on' : ''}`}><span />LIVE</span></div>
          {screen === 'history' && <form className="history-search" onSubmit={searchHistory}><label htmlFor="history-id">ID de activación o correlationId</label><div className="search-row"><input id="history-id" value={historyId} onChange={(event) => setHistoryId(event.target.value)} placeholder="Pegá el identificador" required /><button className="search-button" type="submit" disabled={busy}>{busy ? 'Buscando…' : 'Consultar'}</button></div></form>}
          {activations.length > 0 && <div className="activation-picker" aria-label="Activaciones seguidas">{activations.map((item) => <button className={`activation-chip ${selected?.correlationId === item.correlationId ? 'is-selected' : ''}`} key={item.correlationId} onClick={() => setSelectedId(item.correlationId)}><span>{item.customerId || 'Cliente'}</span><strong className={`status-tag status-tag--${item.status.toLowerCase()}`}>{item.status}</strong></button>)}</div>}

          {selected ? <>
            <div className="activation-meta"><div><span>ACTIVACIÓN</span><strong>{selected.activationId}</strong></div><div><span>CORRELATION ID</span><strong>{selected.correlationId}</strong></div><div><span>ESTADO</span><strong className={`status-text status-text--${selected.status.toLowerCase()}`}>{selected.status}</strong></div></div>
            {selected.planId && <p className="plan-summary">{selected.customerId} <span>·</span> {selected.planId}</p>}
            {selected.events.length ? <ol className="event-list">{selected.events.map((event, index) => <li className="event-item" key={event.eventId || `${event.eventType}-${event.occurredAt}-${index}`}><span className={`event-marker event-marker--${event.eventType.replace(/([a-z])([A-Z])/g, '$1-$2').toLowerCase()}`} /><div className="event-copy"><div className="event-title-row"><strong>{eventLabel(event)}</strong><time>{new Date(event.occurredAt).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}</time></div><span className="event-type">{event.eventType}</span>{'reason' in event.payload && <p className="event-detail">{event.payload.reason}</p>}</div></li>)}</ol> : <div className="empty-events"><span className="empty-line" />Esperando eventos de esta activación…</div>}
          </> : <div className="empty-state"><div className="empty-glyph">↳</div><h3>{screen === 'history' ? 'Buscá una activación' : 'Todavía no hay actividad'}</h3><p>{screen === 'history' ? 'Ingresá un identificador para recuperar su estado e historial.' : 'Al iniciar una solicitud, sus eventos aparecerán acá en orden.'}</p></div>}
          <div className="timeline-footer"><span>FUENTE</span><strong>activation-api</strong><span className="footer-divider" /><span>TRANSPORTE</span><strong>WebSocket</strong></div>
        </section>
      </section>
      <footer className="page-footer"><span>ACTIVACIÓN DE SERVICIOS <b>·</b> POC KAFKA</span><span>TRAZABILIDAD DISTRIBUIDA</span></footer>
    </main>
  </div>
}

export default App