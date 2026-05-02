const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  database: process.env.DB_NAME,

  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

console.log('[DB] pool created');

module.exports = {
  exec: async (sql, params = []) => {
    try {
      const [rows] = await pool.execute(sql, params);
      return rows; // ✔ return rows only
    } catch (err) {
      console.error('[DB ERROR]', {
        message: err.message,
        sql,
        params
      });
      throw err;
    }
  }
};