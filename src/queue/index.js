const { Queue } = require('bullmq');
const IORedis = require('ioredis');

// const connection = new IORedis(process.env.REDIS_URL);
// const connection = new IORedis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');
const connection = new IORedis({
  host: '127.0.0.1',
  port: 6379,
  maxRetriesPerRequest: null
});

const ingestQueue = new Queue('ingest', {
  connection
});

module.exports = {
  ingestQueue,
  connection
};