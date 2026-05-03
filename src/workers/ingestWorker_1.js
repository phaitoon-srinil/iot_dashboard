require('dotenv').config();

const { Worker } = require('bullmq');
const { connection } = require('../queue');
const db = require('../services/db');

console.log('[WORKER MERGED] started...');

// ===================== AUTO CLOSE WATERING =====================
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
}, 5 * 60 * 1000);

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
    console.warn('[VID NOT FOUND]', { pid, deviceAddr, relayNo });
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

  console.log('[IRRIGATION]', { wid, hourTs, etc, rain, net, decision });
}

// ===================== HANDLERS =====================

// ---------- VALVE ----------
async function handleValve({ payload, pid, deviceAddr, relayNo }) {

  if (relayNo === null) return;

  const state = payload?.relay;
  if (state !== 0 && state !== 1) return;

  const vid = await resolveVid(pid, deviceAddr, relayNo);
  if (!vid) return;

  const ts = toMySQLDateTime(payload?.ts ? parseDateSafe(payload.ts) : new Date());

  if (state === 1) {
    try {
      await db.exec(
        `INSERT INTO watering
         (start_at, vid, device_addr, relay_no, source, reason)
         VALUES (?, ?, ?, ?, ?, ?)`,
        [ts, vid, deviceAddr, relayNo, 'mqtt', 'status_on']
      );

      console.log('[WATERING ON]', { vid });

    } catch (e) {
      if (e.code !== 'ER_DUP_ENTRY') {
        console.error('[WATERING ERROR]', e.message);
      }
    }
  }

  if (state === 0) {
    await db.exec(
      `UPDATE watering
       SET stop_at = ?
       WHERE vid = ?
       AND stop_at IS NULL`,
      [ts, vid]
    );

    console.log('[WATERING OFF]', { vid });
  }
}

// ---------- WEATHER ----------
async function handleWeather({ payload, pid, deviceAddr }) {

  const wid = await resolveWid(pid, deviceAddr);
  if (!wid) return;

  const s = payload?.sensor?.[0];
  if (!s) return;

  const ts = toMySQLDateTime(parseDateSafe(s.date));
  const hourTs = toHourStart(ts);

  // RAW
  await db.exec(
    `INSERT INTO climate
     (temperature, humidity, vpd, dew_point, wind_speed,
      wind_gust, wind_direction, wind_max_day,
      solar_radiation, uv_index, rain_day, measured_at, wid)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON DUPLICATE KEY UPDATE
       temperature = VALUES(temperature),
       humidity = VALUES(humidity)`,
    [
      s.temperature,
      s.humidity,
      s.vpd ?? null,
      s.dew_point ?? null,
      s.wind_speed,
      s.wind_gust,
      s.wind_direction,
      s.wind_max_day,
      s.solar_radiation,
      s.uv_index,
      s.rain_day,
      ts,
      wid
    ]
  );

  // RAIN DIFF
  const currentRain = s.rain_day ?? 0;

  const prev = await db.exec(
    `SELECT rain_day FROM hourly_weather
     WHERE wid = ?
     ORDER BY measured_at DESC
     LIMIT 1`,
    [wid]
  );

  let hourlyRain = 0;

  if (prev.length) {
    const prevRain = prev[0].rain_day ?? 0;
    hourlyRain = currentRain < prevRain ? currentRain : currentRain - prevRain;
  }

  const eto = (s.solar_radiation ?? 0) * 0.0005;

  await db.exec(
    `INSERT INTO hourly_weather
     (temperature, humidity, wind_speed, solar_radiation, eto, vpd, hourly_rain, rain_day, measured_at, wid, sample_count)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
     ON DUPLICATE KEY UPDATE
       temperature = (temperature * sample_count + VALUES(temperature)) / (sample_count + 1),
       humidity = (humidity * sample_count + VALUES(humidity)) / (sample_count + 1),
       wind_speed = (wind_speed * sample_count + VALUES(wind_speed)) / (sample_count + 1),
       solar_radiation = (solar_radiation * sample_count + VALUES(solar_radiation)) / (sample_count + 1),
       eto = (eto * sample_count + VALUES(eto)) / (sample_count + 1),
       vpd = (vpd * sample_count + VALUES(vpd)) / (sample_count + 1),
       hourly_rain = VALUES(hourly_rain),
       rain_day = VALUES(rain_day),
       sample_count = sample_count + 1`,
    [
      s.temperature,
      s.humidity,
      s.wind_speed,
      s.solar_radiation,
      eto,
      s.vpd ?? null,
      hourlyRain,
      currentRain,
      hourTs,
      wid
    ]
  );

  console.log('[WEATHER DONE]', { wid, hourTs, eto, hourlyRain });

  // ✅ irrigation trigger เฉพาะ weather
  await computeIrrigation(wid, hourTs);
}

// ---------- SOIL ----------
async function handleSoil({ payload, pid, deviceAddr }) {

  const wid = await resolveWid(pid, deviceAddr);
  if (!wid) return;

  const s = payload?.sensor?.[0];
  if (!s || !Array.isArray(s.data)) return;

  const ts = toMySQLDateTime(parseDateSafe(s.date));

  for (const item of s.data) {
    const sid = await resolveSid(pid, item.addr);
    if (!sid) continue;

    await db.exec(
      `INSERT INTO moisture
       (moisture, temperature, ph, ec_us_cm, measured_at, sid)
       VALUES (?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         moisture = VALUES(moisture)`,
      [
        item.moisture,
        item.temperature,
        item.pH ?? null,
        item.ec ?? null,
        ts,
        sid
      ]
    );
  }c

  console.log('[SOIL DONE]', { wid });
}

// ===================== WORKER =====================
const worker = new Worker(
  'ingest',
  async (job) => {

    const { topic, payload } = job.data;
    const parts = topic.split('/').filter(Boolean);

    if (parts.length < 7) return;

    const pid = parsePid(parts, topic);
    if (!pid) return;

    const deviceKind = parts[3];
    const deviceAddr = Number(parts[4]);
    const relayNo = parts[7] ? Number(parts[7]) : null;

    console.log('[INGEST]', { pid, deviceKind, deviceAddr });

    if (deviceKind === 'relayBoard') {
      await handleValve({ payload, pid, deviceAddr, relayNo });
    }

    if (deviceKind === 'weatherStation') {
      await handleWeather({ payload, pid, deviceAddr });
    }

    if (deviceKind === 'soilSensor') {
      await handleSoil({ payload, pid, deviceAddr });
    }
  },
  { connection, concurrency: 10 }
);