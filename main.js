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
         * @type {Map<string, {dp: {id:string,alias:string,intervalSec:number}, value:any, type:string, unit:string, timestamp:number, timer:NodeJS.Timeout|null}>}
         */
        this.cache = new Map();

        this.apiServer = null;

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
            for (const entry of this.cache.values()) {
                if (entry.timer) clearInterval(entry.timer);
            }
            this.cache.clear();

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

        const alias      = dp.alias.trim();
        const intervalMs = Math.max(1, (dp.intervalSec || 30)) * 1000;

        // Metadaten laden (Einheit)
        let unit = '';
        try {
            const obj = await this.getForeignObjectAsync(dp.id);
            if (obj && obj.common && obj.common.unit) unit = obj.common.unit;
        } catch (e) {
            this.log.debug(`Metadaten für ${dp.id} nicht abrufbar: ${e.message}`);
        }

        // Cache-Eintrag anlegen
        const entry = { dp, value: null, type: 'mixed', unit, timestamp: 0, timer: null };
        this.cache.set(alias, entry);

        // Ersten Wert sofort lesen
        await this.readAndCacheState(alias);

        // Echtzeit-Subscription
        try {
            await this.subscribeForeignStatesAsync(dp.id);
        } catch (e) {
            this.log.debug(`Subscription für ${dp.id} fehlgeschlagen: ${e.message}`);
        }

        // Interval-Timer
        entry.timer = setInterval(() => {
            this.readAndCacheState(alias).catch(e =>
                this.log.warn(`Intervalfehler ${dp.id}: ${e.message}`)
            );
        }, intervalMs);

        this.log.info(`Datenpunkt "${alias}" (${dp.id}) alle ${dp.intervalSec || 30}s | Einheit: ${unit || '–'}`);
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

    onStateChange(id, state) {
        // Echtzeit-Update für alle konfigurierten Datenpunkte
        for (const [alias, entry] of this.cache.entries()) {
            if (entry.dp.id === id && state) {
                entry.value     = state.val;
                entry.type      = this.detectType(state.val);
                entry.timestamp = state.ts || Date.now();
                this.log.debug(`Echtzeit-Update: "${alias}" = ${state.val}`);
            }
        }
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
                status:     'ok',
                adapter:    'iosync',
                serverTime: Date.now(),
                datapoints: this.cache.size
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

            // Vollständiger Objektbaum via getObjectViewAsync (Admin-UI Datenpunkt-Browser)
            case 'getObjectTree': {
                this.getObjectViewAsync('system', 'state', { startkey: '', endkey: '\u9999' })
                    .then(result => {
                        const results = [];
                        if (result && result.rows) {
                            for (const row of result.rows) {
                                const id = row.id;
                                const o  = row.value;
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
