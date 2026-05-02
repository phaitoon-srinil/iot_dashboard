require('dotenv').config();

const { Worker } = require('bullmq');
const { connection } = require('../queue');
const db = require('../services/db');

console.log('[WORKER V6] started...');

// ===================== AUTO CLOSE (🔥 NEW) =====================
setInterval(async () => {
  try {
    const res = await db.exec(`
      UPDATE watering
      SET 
        stop_at = DATE_ADD(start_at, INTERVAL 6 HOUR),
        reason = 'SYSTEM'
      WHERE stop_at IS NULL
      AND start_at < NOW() - INTERVAL 6 HOUR
    `);

    console.log('[AUTO CLOSE WATERING]', {
      affectedRows: res?.affectedRows ?? 0
    });

  } catch (e) {
    console.error('[AUTO CLOSE ERROR]', e.message);
  }
}, 5 * 60 * 1000); // ทุก 5 นาที


// ===================== DATE =====================
function parseDateSafe(input) {
  if (!input) return new Date();

  const str = String(input).trim();
  const iso = new Date(str);
  if (!isNaN(iso)) return iso;

  const [datePart, timePart] = str.split(' ');
  if (!datePart || !timePart) return new Date();

  const d = datePart.split('/');
  const t = timePart.split(':');

  return new Date(
    2000 + parseInt(d[0], 10),
    parseInt(d[1], 10) - 1,
    parseInt(d[2], 10),
    parseInt(t[0], 10),
    parseInt(t[1], 10),
    parseInt(t[2], 10)
  );
}

function toMySQLDateTime(d) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function toHourStart(ts) {
  return ts.substring(0, 13) + ':00:00';
}

// ===================== VALIDATION =====================
function parsePid(parts, topic) {
  const pid = Number(parts[2]);
  if (isNaN(pid)) {
    console.warn('[INVALID PID]', topic);
    return null;
  }
  return pid;
}

// ===================== LOOKUP =====================
async function resolveWid(pid, modbusId) {
  const rows = await db.exec(
    `SELECT w.wid
     FROM weatherStation w
     JOIN zone z ON w.zid = z.zid
     WHERE w.modbus_id = ?
       AND z.pid = ?
     LIMIT 1`,
    [modbusId, pid]
  );

  if (!rows.length) {
    console.warn('[WID NOT FOUND]', { pid, modbusId });
    return null;
  }

  return rows[0].wid;
}

async function resolveVid(pid, deviceAddr, relayNo) {
  const rows = await db.exec(
    `SELECT v.vid
     FROM valve v
     JOIN zone z ON v.zid = z.zid
     WHERE z.pid = ?
       AND v.device_addr = ?
       AND v.relay_no = ?
     LIMIT 1`,
    [pid, deviceAddr, relayNo]
  );

  if (!rows.length) {
    console.log('[VID NOT FOUND]', { pid, deviceAddr, relayNo });
    return null;
  }

  return rows[0].vid;
}

async function resolveSid(pid, modbusId) {
  const rows = await db.exec(
    `SELECT s.sid
     FROM soilSensor s
     JOIN zone z ON s.zid = z.zid
     WHERE s.modbus_id = ?
       AND z.pid = ?
     LIMIT 1`,
    [modbusId, pid]
  );

  if (!rows.length) {
    console.warn('[SID NOT FOUND]', { pid, modbusId });
    return null;
  }

  return rows[0].sid;
}

// ===================== IRRIGATION =====================
async function computeIrrigation(wid, hourTs) {

  const rows = await db.exec(
    `SELECT eto, hourly_rain FROM hourly_weather
     WHERE wid = ? AND measured_at = ? LIMIT 1`,
    [wid, hourTs]
  );

  if (!rows.length) return;

  const eto = rows[0].eto ?? 0;
  const rain = rows[0].hourly_rain ?? 0;

  const soilRows = await db.exec(
    `SELECT AVG(moisture_avg) AS m
     FROM hourly_moisture
     WHERE wid = ? AND measured_at = ?`,
    [wid, hourTs]
  );

  const soilAvg = soilRows[0]?.m ?? null;

  const kc = 0.85;
  const etc = eto * kc;

  const fieldCapacity = 35;
  const wiltingPoint = 15;

  let depletion = null;
  if (soilAvg !== null) {
    depletion = (fieldCapacity - soilAvg) / (fieldCapacity - wiltingPoint);
  }

  let decision = 'skip';
  if (etc > 0.2 && (depletion === null || depletion > 0.4)) {
    decision = 'irrigate';
  }

  const net = Math.max(0, etc - rain) * (1 + (depletion ?? 0));

  await db.exec(
    `INSERT INTO hourly_irrigation
     (kc, etc, rain, pe, net, pe_factor, vpd, measured_at, wid)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       kc = VALUES(kc),
       etc = VALUES(etc),
       net = VALUES(net)`,
    [kc, etc, rain, 0, net, 0.8, null, hourTs, wid]
  );

  console.log('[IRRIGATION]', { wid, hourTs, eto, rain, soilAvg, decision, net });
}

// ===================== WORKER =====================
const worker = new Worker(
  'ingest',
  async (job) => {

    const { topic, payload } = job.data;
    const parts = topic.split('/').filter(Boolean);

    if (parts.length < 8) return;

    const dataType = parts[6];
    const relayNo = parts[7] ? Number(parts[7]) : null;
    const pid = parsePid(parts, topic);
    if (!pid) return;

    const deviceKind = parts[3];
    const deviceAddr = Number(parts[4]);

    console.log('[INGEST]', { pid, deviceKind, deviceAddr });

    // ================= VALVE =================
    if (deviceKind === 'relayBoard' && dataType === 'relay') {

      if (relayNo === null) return;

      const state = payload?.relay;
      if (state !== 0 && state !== 1) return;

      const vid = await resolveVid(pid, deviceAddr, relayNo);
      if (!vid) return;

      const eventTime = payload?.ts
        ? parseDateSafe(payload.ts)
        : new Date();

      const ts = toMySQLDateTime(eventTime);

      if (state === 1) {
        try {

          const reason =
            payload.reason && typeof payload.reason === 'string'
              ? payload.reason
              : 'status_on';

          const source =
            payload.source && typeof payload.source === 'string'
              ? payload.source
              : 'mqtt';

          await db.exec(
            `INSERT INTO watering
             (start_at, vid, device_addr, relay_no, source, reason)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [ts, vid, deviceAddr, relayNo, source, reason]
          );

          console.log('[WATERING ON]', { vid });

        } catch (e) {

          if (e.code === 'ER_DUP_ENTRY') {
            console.log('[WATERING ON DUPLICATE IGNORE]', { vid });
          } else {
            console.error('[WATERING ON ERROR]', e.message);
          }
        }
      }

      if (state === 0) {

        const res = await db.exec(
          `UPDATE watering
           SET stop_at = ?
           WHERE vid = ?
           AND stop_at IS NULL`,
          [ts, vid]
        );

        if (res.affectedRows === 1) {
          console.log('[WATERING OFF]', { vid });
        } else {
          console.log('[WATERING OFF IGNORE]', { vid });
        }
      }
    }
  },
  { connection, concurrency: 10 }
);