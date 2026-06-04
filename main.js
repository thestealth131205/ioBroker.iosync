'use strict';

/**
 * IoSync ioBroker Adapter — Data Point Broker
 *
 * Stellt konfigurierte ioBroker-Datenpunkte über eine HTTPS-API bereit.
 * Smartwatches, andere Apps oder Dienste können die Daten per HTTP-GET abrufen.
 *
 * Konfiguration (Admin-UI):
 *   apiPort          Port des HTTPS-Servers (Standard: 7443)
 *   apiUsername      Benutzername für Basic Auth
 *   apiPassword      Passwort für Basic Auth
 *   apiSslCertPath   Pfad zum SSL-Zertifikat (leer = Auto-Generierung)
 *   apiSslKeyPath    Pfad zum SSL-Schlüssel (leer = Auto-Generierung)
 *   dataPoints       Array von { id, alias, intervalSec }
 */

const utils   = require('@iobroker/adapter-core');
const https   = require('https');
const http    = require('http');
const fs      = require('fs');
const path    = require('path');
const express = require('express');

// selfsigned ist optional — nur für Auto-SSL nötig
let selfsigned;
try { selfsigned = require('selfsigned'); } catch { selfsigned = null; }

const SSL_DIR = path.join(__dirname, 'ssl');

class IoSyncAdapter extends utils.Adapter {

    constructor(options) {
        super({ ...options, name: 'iosync' });

        /**
         * Cache aller konfigurierten Datenpunkte.
         * Key = dp.alias
         * @type {Map<string, {dp: {id:string,alias:string,intervalSec:number}, value:any, type:string, unit:string, timestamp:number, lastStateChange:number}>}
         */
        this.cache = new Map();

        this.apiServer = null;

        /**
         * Aktive Server-Sent-Events-Clients (Echtzeit-Push).
         * @type {Set<import('http').ServerResponse>}
         */
        this.sseClients = new Set();
        this.sseHeartbeat = null;

        this.on('ready',       this.onReady.bind(this));
        this.on('stateChange', this.onStateChange.bind(this));
        this.on('message',     this.onMessage.bind(this));
        this.on('unload',      this.onUnload.bind(this));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ─────────────────────────────────────────────────────────────────────────

    async onReady() {
        this.log.info('IoSync Broker startet…');
        await this.setStateAsync('info.connection', { val: false, ack: true });

        const dps = Array.isArray(this.config.dataPoints) ? this.config.dataPoints : [];
        this.log.info(`${dps.length} Datenpunkt(e) konfiguriert`);

        for (const dp of dps) {
            await this.initDataPoint(dp);
        }

        await this.startApiServer();
        this.log.info('IoSync Broker bereit');
    }

    onUnload(callback) {
        try {
            this.log.info('IoSync Broker wird gestoppt…');
            this.cache.clear();

            if (this.sseHeartbeat) { clearInterval(this.sseHeartbeat); this.sseHeartbeat = null; }
            for (const res of this.sseClients) {
                try { res.end(); } catch (e) { /* ignore */ }
            }
            this.sseClients.clear();

            const finish = () => {
                this.setStateAsync('info.connection', { val: false, ack: true })
                    .finally(() => callback());
            };

            if (this.apiServer && this.apiServer.listening) {
                this.apiServer.close(finish);
            } else {
                finish();
            }
        } catch (e) {
            callback();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Datenpunkt-Verwaltung
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Initialisiert einen Datenpunkt: liest Metadaten, cached aktuellen Wert,
     * startet Interval-Timer.
     * @param {{ id: string, alias: string, intervalSec: number }} dp
     */
    async initDataPoint(dp) {
        if (!dp.id || !dp.alias) {
            this.log.warn(`Datenpunkt übersprungen (fehlende id oder alias): ${JSON.stringify(dp)}`);
            return;
        }

        const alias = dp.alias.trim();

        // Metadaten laden (Einheit)
        let unit = '';
        try {
            const obj = await this.getForeignObjectAsync(dp.id);
            if (obj && obj.common && obj.common.unit) unit = obj.common.unit;
        } catch (e) {
            this.log.debug(`Metadaten für ${dp.id} nicht abrufbar: ${e.message}`);
        }

        // Cache-Eintrag anlegen
        const entry = { dp, value: null, type: 'mixed', unit, timestamp: 0, lastStateChange: 0 };
        this.cache.set(alias, entry);

        // Ersten Wert sofort lesen (Startwert)
        await this.readAndCacheState(alias);

        // Echtzeit-Subscription — Updates erfolgen ausschließlich event-basiert bei Änderung
        try {
            await this.subscribeForeignStatesAsync(dp.id);
        } catch (e) {
            this.log.debug(`Subscription für ${dp.id} fehlgeschlagen: ${e.message}`);
        }

        this.log.info(`Datenpunkt "${alias}" (${dp.id}) | Echtzeit (nur bei Änderung) | Einheit: ${unit || '–'}`);
    }

    /**
     * Liest aktuellen Zustand aus ioBroker und aktualisiert den Cache.
     * @param {string} alias
     */
    async readAndCacheState(alias) {
        const entry = this.cache.get(alias);
        if (!entry) return;
        try {
            const state = await this.getForeignStateAsync(entry.dp.id);
            if (state !== null && state !== undefined) {
                entry.value     = state.val;
                entry.type      = this.detectType(state.val);
                entry.timestamp = state.ts || Date.now();
            }
        } catch (e) {
            this.log.debug(`Lesefehler ${entry.dp.id}: ${e.message}`);
        }
    }

    /**
     * Echtzeit-Update bei Wertänderung. Es gibt KEIN Zeitintervall — ein Wert wird
     * ausschließlich dann übernommen (und damit für die App bereitgestellt), wenn er
     * sich tatsächlich relevant geändert hat:
     *   • Boolean   → nur bei echtem Wechsel (true ↔ false)
     *   • Dezimal   → nur bei Abweichung von mindestens 0.2
     *   • Ganzzahl  → nur bei Abweichung von mindestens 1
     *   • sonstiges → nur bei Wertänderung
     */
    onStateChange(id, state) {
        if (!state) return;
        const now = Date.now();

        for (const [alias, entry] of this.cache.entries()) {
            if (entry.dp.id !== id) continue;

            const prevValue = entry.value;
            const newValue  = state.val;
            let send = true;

            // Erster Wert oder Typwechsel → immer übernehmen
            if (prevValue !== null && prevValue !== undefined && typeof prevValue === typeof newValue) {
                if (typeof newValue === 'boolean') {
                    // Boolean: nur senden, wenn der Wert wirklich umgeschaltet wurde
                    send = newValue !== prevValue;
                } else if (typeof newValue === 'number') {
                    // Dezimalzahl → Schwelle 0.2, Ganzzahl → Schwelle 1
                    const isDecimal = !Number.isInteger(newValue) || !Number.isInteger(prevValue);
                    send = Math.abs(newValue - prevValue) >= (isDecimal ? 0.2 : 1);
                } else {
                    // String / sonstiges: nur bei tatsächlicher Änderung
                    send = newValue !== prevValue;
                }
            }

            if (!send) continue;

            entry.value           = newValue;
            entry.type            = this.detectType(newValue);
            entry.timestamp       = state.ts || now;
            entry.lastStateChange = now;
            this.log.debug(`Echtzeit-Update: "${alias}" = ${newValue}`);

            // Echtzeit-Push an verbundene Clients (SSE) — nur bei aktivierter Push-Option
            this.broadcastSse(alias, entry);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Echtzeit-Push (Server-Sent Events)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Sendet ein einzelnes Datenpunkt-Update an alle verbundenen SSE-Clients.
     * Wird ausschließlich bei einer relevanten Wertänderung aufgerufen.
     * @param {string} alias
     * @param {object} entry
     */
    broadcastSse(alias, entry) {
        if (this.config.pushEnabled === false) return;
        if (this.sseClients.size === 0) return;

        const payload = JSON.stringify(this.buildApiPayload(alias, entry));
        const frame   = `event: update\ndata: ${payload}\n\n`;

        for (const res of this.sseClients) {
            try {
                res.write(frame);
            } catch (e) {
                this.sseClients.delete(res);
            }
        }
        this.log.debug(`Push: "${alias}" an ${this.sseClients.size} Client(s) gesendet`);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HTTPS-API-Server
    // ─────────────────────────────────────────────────────────────────────────

    async startApiServer() {
        const port     = parseInt(this.config.apiPort) || 7443;
        const username = (this.config.apiUsername || '').trim();
        const password = (this.config.apiPassword || '').trim();

        const app = express();

        // CORS für lokale Clients (Smartwatch-Browser, Apps)
        app.use((_req, res, next) => {
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
            res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
            if (_req.method === 'OPTIONS') return res.sendStatus(204);
            next();
        });

        app.use(express.json());

        // Basic Auth Middleware
        if (username && password) {
            app.use((req, res, next) => {
                const authHeader = req.headers['authorization'] || '';
                if (authHeader.startsWith('Basic ')) {
                    const decoded = Buffer.from(authHeader.slice(6), 'base64').toString('utf8');
                    const sep     = decoded.indexOf(':');
                    const u = decoded.slice(0, sep);
                    const p = decoded.slice(sep + 1);
                    if (u === username && p === password) return next();
                }
                res.setHeader('WWW-Authenticate', 'Basic realm="IoSync API"');
                return res.status(401).json({ error: 'Nicht autorisiert' });
            });
        }

        // ── Routen ────────────────────────────────────────────────────────

        app.get('/api/health', (_req, res) => {
            res.json({
                status:      'ok',
                adapter:     'iosync',
                serverTime:  Date.now(),
                datapoints:  this.cache.size,
                pushEnabled: this.config.pushEnabled !== false
            });
        });

        // Echtzeit-Push via Server-Sent Events. Der Client (z.B. Android-App) hält
        // diese Verbindung offen und erhält bei jeder relevanten Wertänderung sofort
        // ein "update"-Event. Nur aktiv, wenn Push in der Konfiguration eingeschaltet ist.
        app.get('/api/stream', (req, res) => {
            if (this.config.pushEnabled === false) {
                return res.status(404).json({ error: 'Push ist deaktiviert' });
            }

            res.writeHead(200, {
                'Content-Type':      'text/event-stream',
                'Cache-Control':     'no-cache, no-transform',
                'Connection':        'keep-alive',
                'X-Accel-Buffering': 'no'
            });
            res.write('retry: 5000\n\n');
            // Aktuellen Stand sofort einmalig senden, damit der Client nicht erst auf
            // die nächste Änderung warten muss.
            for (const [alias, entry] of this.cache.entries()) {
                res.write(`event: update\ndata: ${JSON.stringify(this.buildApiPayload(alias, entry))}\n\n`);
            }

            this.sseClients.add(res);
            this.log.info(`SSE-Client verbunden (${this.sseClients.size} aktiv)`);

            req.on('close', () => {
                this.sseClients.delete(res);
                this.log.info(`SSE-Client getrennt (${this.sseClients.size} aktiv)`);
            });
        });

        app.get('/api/datapoints', (_req, res) => {
            const result = [];
            for (const [alias, entry] of this.cache.entries()) {
                result.push(this.buildApiPayload(alias, entry));
            }
            res.json({ datapoints: result, serverTime: Date.now(), count: result.length });
        });

        app.get('/api/datapoints/:alias', (req, res) => {
            const alias = decodeURIComponent(req.params.alias);
            const entry = this.cache.get(alias);
            if (!entry) {
                return res.status(404).json({ error: `Datenpunkt "${alias}" nicht gefunden` });
            }
            res.json(this.buildApiPayload(alias, entry));
        });

        // Schreibt einen Wert in ioBroker — aufgerufen von der Android-App
        app.post('/api/setState', async (req, res) => {
            const { id, value } = req.body || {};
            if (!id || value === undefined) {
                return res.status(400).json({ error: '"id" und "value" sind erforderlich' });
            }
            try {
                // Typkonvertierung: versuche Boolean/Number zu erkennen
                let val = value;
                if (value === 'true')       val = true;
                else if (value === 'false') val = false;
                else if (!isNaN(value) && value !== '') val = Number(value);

                await this.setForeignStateAsync(id, { val, ack: false });
                this.log.info(`setState via Android: ${id} = ${val}`);
                res.json({ ok: true, id, value: val });
            } catch (e) {
                this.log.error(`setState ${id} fehlgeschlagen: ${e.message}`);
                res.status(500).json({ error: e.message });
            }
        });

        // Android → Adapter: aktuellen Wert eines Datenpunkts direkt abfragen (ohne Cache)
        app.get('/api/state/:id(*)', async (req, res) => {
            const id = req.params.id;
            try {
                const state = await this.getForeignStateAsync(id);
                if (!state) return res.status(404).json({ error: `Datenpunkt "${id}" nicht gefunden` });
                res.json({ id, value: state.val, ts: state.ts, ack: state.ack });
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        // Alle ioBroker-State-Objekte für Admin-UI-Browser (kein Auth nötig — lokaler Zugriff)
        app.get('/api/stateObjects', async (_req, res) => {
            try {
                const objects = await this.getForeignObjectsAsync('*', 'state');
                const results = [];
                for (const [id, o] of Object.entries(objects || {})) {
                    if (!o || !o.common) continue;
                    const rawName = o.common.name;
                    const name = rawName && typeof rawName === 'object'
                        ? (rawName.de || rawName.en || id)
                        : (rawName || id);
                    results.push({
                        id,
                        name:  String(name) !== id ? String(name) : '',
                        unit:  o.common.unit  || '',
                        type:  o.common.type  || 'mixed',
                        role:  o.common.role  || ''
                    });
                }
                results.sort((a, b) => a.id.localeCompare(b.id));
                res.json({ results });
            } catch (e) {
                res.status(500).json({ error: e.message, results: [] });
            }
        });

        // Konfiguration aus Admin-UI speichern (direkt über Adapter)
        app.post('/api/saveConfig', express.json(), async (req, res) => {
            const settings = req.body;
            if (!settings || typeof settings !== 'object') {
                return res.status(400).json({ error: 'Ungültige Konfiguration' });
            }
            try {
                const adapterId = 'system.adapter.iosync.' + this.instance;
                await this.extendForeignObjectAsync(adapterId, { native: settings });
                res.json({ ok: true });
            } catch (e) {
                res.status(500).json({ error: e.message });
            }
        });

        app.use((_req, res) => {
            res.status(404).json({ error: 'Endpunkt nicht gefunden' });
        });

        // ── HTTP-Modus oder SSL ───────────────────────────────────────────
        if (this.config.httpMode) {
            this.log.info('HTTP-Modus aktiv (kein SSL) — Apache übernimmt TLS-Terminierung');
            this.apiServer = http.createServer(app);
            this.apiServer.listen(port, () => {
                this.log.info(`HTTP-API läuft auf Port ${port}`);
                this.setStateAsync('info.connection', { val: true, ack: true });
                this.setStateAsync('info.apiPort',    { val: port, ack: true });
            });
        } else {
            try {
                const sslCreds = await this.loadOrGenerateSsl();
                this.apiServer = https.createServer(sslCreds, app);
                this.apiServer.listen(port, () => {
                    this.log.info(`HTTPS-API läuft auf Port ${port}`);
                    this.setStateAsync('info.connection', { val: true, ack: true });
                    this.setStateAsync('info.apiPort',    { val: port, ack: true });
                });
            } catch (err) {
                this.log.error(`HTTPS konnte nicht gestartet werden: ${err.message}`);
                this.log.warn('Fallback: HTTP-Server ohne SSL…');
                this.apiServer = http.createServer(app);
                this.apiServer.listen(port, () => {
                    this.log.warn(`HTTP-API (kein SSL, Fallback) läuft auf Port ${port}`);
                    this.setStateAsync('info.connection', { val: true, ack: true });
                    this.setStateAsync('info.apiPort',    { val: port, ack: true });
                });
            }
        }

        this.apiServer.on('error', (err) => {
            this.log.error(`API-Server-Fehler: ${err.message}`);
            this.setStateAsync('info.connection', { val: false, ack: true });
        });

        // Heartbeat hält SSE-Verbindungen über Proxies/NAT offen (Kommentarzeile).
        if (!this.sseHeartbeat) {
            this.sseHeartbeat = setInterval(() => {
                for (const res of this.sseClients) {
                    try { res.write(': ping\n\n'); }
                    catch (e) { this.sseClients.delete(res); }
                }
            }, 25_000);
        }
    }

    /**
     * Lädt SSL-Zertifikat aus konfigurierten Pfaden oder generiert selbstsigniertes.
     * @returns {Promise<{cert: string, key: string}>}
     */
    async loadOrGenerateSsl() {
        const certPath = (this.config.apiSslCertPath || '').trim();
        const keyPath  = (this.config.apiSslKeyPath  || '').trim();

        if (certPath && keyPath) {
            if (!fs.existsSync(certPath)) throw new Error(`Zertifikat nicht gefunden: ${certPath}`);
            if (!fs.existsSync(keyPath))  throw new Error(`Schlüssel nicht gefunden: ${keyPath}`);
            this.log.info(`SSL: Lade Zertifikat aus ${certPath}`);
            return {
                cert: fs.readFileSync(certPath, 'utf8'),
                key:  fs.readFileSync(keyPath,  'utf8')
            };
        }

        const cachedCert = path.join(SSL_DIR, 'cert.pem');
        const cachedKey  = path.join(SSL_DIR, 'key.pem');

        if (fs.existsSync(cachedCert) && fs.existsSync(cachedKey)) {
            this.log.info('SSL: Verwende gecachtes selbstsigniertes Zertifikat');
            return {
                cert: fs.readFileSync(cachedCert, 'utf8'),
                key:  fs.readFileSync(cachedKey,  'utf8')
            };
        }

        if (!selfsigned) {
            throw new Error(
                'Paket "selfsigned" nicht installiert und keine SSL-Pfade konfiguriert. ' +
                'Bitte "npm install" im Adapter-Verzeichnis ausführen oder SSL-Pfade angeben.'
            );
        }

        this.log.info('SSL: Generiere selbstsigniertes Zertifikat (einmalig)…');
        const attrs = [
            { name: 'commonName',       value: 'IoSync Adapter' },
            { name: 'organizationName', value: 'IoSync'         },
            { name: 'countryName',      value: 'DE'             }
        ];
        const pems = selfsigned.generate(attrs, {
            days:       3650,
            keySize:    2048,
            algorithm:  'sha256',
            extensions: [{
                name: 'subjectAltName',
                altNames: [
                    { type: 7, ip: '127.0.0.1' },
                    { type: 2, value: 'localhost' }
                ]
            }]
        });

        if (!fs.existsSync(SSL_DIR)) fs.mkdirSync(SSL_DIR, { recursive: true });
        fs.writeFileSync(cachedCert, pems.cert,    'utf8');
        fs.writeFileSync(cachedKey,  pems.private, 'utf8');
        this.log.info(`SSL: Selbstsigniertes Zertifikat in ${SSL_DIR} gespeichert`);

        return { cert: pems.cert, key: pems.private };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Admin-UI Nachrichten
    // ─────────────────────────────────────────────────────────────────────────

    onMessage(obj) {
        if (!obj || !obj.command) return;

        switch (obj.command) {

            // Vollständiger Objektbaum via getForeignObjectsAsync (Admin-UI Datenpunkt-Browser)
            case 'getObjectTree': {
                this.getForeignObjectsAsync('*', 'state')
                    .then(objects => {
                        const results = [];
                        for (const [id, o] of Object.entries(objects || {})) {
                            if (!o || !o.common) continue;
                            const rawName = o.common.name;
                            const name = rawName && typeof rawName === 'object'
                                ? (rawName.de || rawName.en || id)
                                : (rawName || id);
                            results.push({
                                id,
                                name: String(name) !== id ? String(name) : '',
                                unit: o.common.unit || '',
                                type: o.common.type || 'mixed',
                                role: o.common.role || ''
                            });
                        }
                        results.sort((a, b) => a.id.localeCompare(b.id));
                        if (obj.callback) {
                            this.sendTo(obj.from, obj.command,
                                { results: results.slice(0, 5000) }, obj.callback);
                        }
                    })
                    .catch(err => {
                        if (obj.callback) {
                            this.sendTo(obj.from, obj.command,
                                { error: err.message, results: [] }, obj.callback);
                        }
                    });
                break;
            }

            // ioBroker-Datenpunkte nach Muster suchen
            case 'searchStates': {
                const pattern = (obj.message && obj.message.pattern) ? obj.message.pattern.trim() : '*';
                this.getForeignObjectsAsync(pattern, 'state').then(objects => {
                    const results = [];
                    for (const [id, o] of Object.entries(objects || {})) {
                        if (!o || !o.common) continue;
                        const rawName = o.common.name;
                        const name = rawName && typeof rawName === 'object'
                            ? (rawName.de || rawName.en || id)
                            : (rawName || id);
                        results.push({
                            id,
                            name:  String(name),
                            unit:  o.common.unit  || '',
                            type:  o.common.type  || 'mixed',
                            role:  o.common.role  || ''
                        });
                    }
                    results.sort((a, b) => a.id.localeCompare(b.id));
                    obj.callback && this.sendTo(obj.from, obj.command,
                        { results: results.slice(0, 3000) }, obj.callback);
                }).catch(err => {
                    obj.callback && this.sendTo(obj.from, obj.command,
                        { error: err.message, results: [] }, obj.callback);
                });
                break;
            }

            // API-Server-Status
            case 'testApi': {
                const running = !!(this.apiServer && this.apiServer.listening);
                obj.callback && this.sendTo(obj.from, obj.command, {
                    running,
                    port:      parseInt(this.config.apiPort) || 7443,
                    cacheSize: this.cache.size
                }, obj.callback);
                break;
            }

            // Cache-Vorschau
            case 'getCachePreview': {
                const preview = [];
                for (const [alias, entry] of this.cache.entries()) {
                    preview.push(this.buildApiPayload(alias, entry));
                }
                obj.callback && this.sendTo(obj.from, obj.command, { datapoints: preview }, obj.callback);
                break;
            }

            default:
                this.log.warn(`Unbekannter Befehl: ${obj.command}`);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Hilfsfunktionen
    // ─────────────────────────────────────────────────────────────────────────

    buildApiPayload(alias, entry) {
        return {
            alias,
            id:          entry.dp.id,
            value:       entry.value,
            type:        entry.type,
            unit:        entry.unit || '',
            timestamp:   entry.timestamp,
            intervalSec: entry.dp.intervalSec || 30,
            age:         entry.timestamp
                            ? Math.round((Date.now() - entry.timestamp) / 1000)
                            : null
        };
    }

    detectType(val) {
        if (typeof val === 'boolean') return 'boolean';
        if (typeof val === 'number')  return 'number';
        if (typeof val === 'string')  return 'string';
        return 'mixed';
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Einstiegspunkt
// ─────────────────────────────────────────────────────────────────────────────

if (require.main !== module) {
    module.exports = (options) => new IoSyncAdapter(options);
} else {
    new IoSyncAdapter();
}
