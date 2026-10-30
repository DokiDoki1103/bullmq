import { default as IORedis } from 'ioredis';
import {
  describe,
  beforeEach,
  afterEach,
  beforeAll,
  afterAll,
  it,
  expect,
} from 'vitest';

import { randomUUID as v4 } from 'crypto';
import { Queue, QueueEvents, RateLimitError, Worker } from '../src/classes';
import { delay, removeAllQueueData } from '../src/utils';

// Calls fn once it has been invoked n times (same semantics as lodash.after).
const after = (n: number, fn: (...args: any[]) => void) => {
  let calls = 0;
  return (...args: any[]) => {
    if (++calls >= n) {
      return fn(...args);
    }
  };
};

describe('Local Rate Limiter (per-worker)', () => {
  const redisHost = process.env.REDIS_HOST || 'localhost';
  const redisPort = Number(process.env.REDIS_PORT || 6379);
  const prefix = process.env.BULLMQ_TEST_PREFIX || 'bull';
  let queue: Queue;
  let queueName: string;
  let queueEvents: QueueEvents;
  let connection: IORedis;

  beforeAll(async () => {
    connection = new IORedis(redisPort, redisHost, {
      maxRetriesPerRequest: null,
    });
  });

  beforeEach(async () => {
    queueName = `test-${v4()}`;
    queue = new Queue(queueName, { connection, prefix });
    queueEvents = new QueueEvents(queueName, { connection, prefix });
    await queueEvents.waitUntilReady();
  });

  afterEach(async () => {
    await queue.close();
    await queueEvents.close();
    await removeAllQueueData(new IORedis(redisPort, redisHost), queueName);
  });

  afterAll(async () => {
    await connection.quit();
  });

  describe('single worker', () => {
    it('should obey its own local rate limit', { timeout: 15000 }, async () => {
      // 1 worker, max 1 job/sec → 10 jobs takes ≥ 9s
      const numJobs = 10;
      const worker = new Worker(queueName, async () => {}, {
        connection,
        prefix,
        localLimiter: { max: 1, duration: 1000 },
      });
      await worker.waitUntilReady();

      const result = new Promise<void>((resolve, reject) => {
        queueEvents.on(
          'completed',
          after(numJobs, () => {
            try {
              const elapsed = Date.now() - startTime;
              expect(elapsed).toBeGreaterThanOrEqual((numJobs - 1) * 1000);
              resolve();
            } catch (err) {
              reject(err);
            }
          }),
        );
        queueEvents.on('failed', async err => {
          await worker.close();
          reject(err);
        });
      });

      const startTime = Date.now();
      await queue.addBulk(
        Array.from({ length: numJobs }, () => ({ name: 'j', data: {} })),
      );

      await result;
      await worker.close();
    });

    it('should use a per-worker Redis key (not the global limiter key)', async () => {
      const worker = new Worker(queueName, async () => {}, {
        connection,
        prefix,
        localLimiter: { max: 1, duration: 5000 },
      });
      await worker.waitUntilReady();

      await queue.add('j', {});
      // give the worker time to pick up the job and increment its key
      await delay(300);

      const globalKey = `${prefix}:${queueName}:limiter`;
      const workerKey = `${globalKey}:${worker.id}`;

      const globalVal = await connection.get(globalKey);
      const workerVal = await connection.get(workerKey);

      // global key must be untouched
      expect(globalVal).toBeNull();
      // per-worker key must have been incremented
      expect(Number(workerVal)).toBeGreaterThanOrEqual(1);

      await worker.close();
    });

    it('should delete the per-worker Redis key on close()', async () => {
      const worker = new Worker(queueName, async () => {}, {
        connection,
        prefix,
        localLimiter: { max: 1, duration: 60_000 },
      });
      await worker.waitUntilReady();

      await queue.add('j', {});
      await delay(300);

      const workerKey = `${prefix}:${queueName}:limiter:${worker.id}`;
      expect(await connection.exists(workerKey)).toBe(1);

      await worker.close();

      // key must be gone after close
      expect(await connection.exists(workerKey)).toBe(0);
    });
  });

  describe('multiple workers (linear throughput scaling)', () => {
    it(
      'should scale throughput linearly: 2 workers × 5/s ≈ 10 jobs/s',
      { timeout: 10000 },
      async () => {
        // With 2 independent local limiters (max:5/1s), 10 jobs should
        // finish in ~1s. With a shared global limiter they would take ~2s.
        const numJobs = 10;
        const workers = [
          new Worker(queueName, async () => {}, {
            connection,
            prefix,
            localLimiter: { max: 5, duration: 1000 },
          }),
          new Worker(queueName, async () => {}, {
            connection,
            prefix,
            localLimiter: { max: 5, duration: 1000 },
          }),
        ];
        await Promise.all(workers.map(w => w.waitUntilReady()));

        const result = new Promise<void>((resolve, reject) => {
          queueEvents.on(
            'completed',
            after(numJobs, () => {
              try {
                const elapsed = Date.now() - startTime;
                // All 10 jobs should complete within 1 rate-limit window + headroom
                expect(elapsed).toBeLessThan(2500);
                resolve();
              } catch (err) {
                reject(err);
              }
            }),
          );
          queueEvents.on('failed', async err => {
            await Promise.all(workers.map(w => w.close()));
            reject(err);
          });
        });

        const startTime = Date.now();
        await queue.addBulk(
          Array.from({ length: numJobs }, () => ({ name: 'j', data: {} })),
        );

        await result;
        await Promise.all(workers.map(w => w.close()));
      },
    );

    it('should give each worker its own independent Redis key', async () => {
      const workers = [
        new Worker(queueName, async () => {}, {
          connection,
          prefix,
          localLimiter: { max: 3, duration: 5000 },
        }),
        new Worker(queueName, async () => {}, {
          connection,
          prefix,
          localLimiter: { max: 3, duration: 5000 },
        }),
      ];
      await Promise.all(workers.map(w => w.waitUntilReady()));

      // The two worker IDs must be distinct
      expect(workers[0].id).not.toBe(workers[1].id);

      await queue.addBulk(
        Array.from({ length: 6 }, () => ({ name: 'j', data: {} })),
      );
      await delay(400);

      const key0 = `${prefix}:${queueName}:limiter:${workers[0].id}`;
      const key1 = `${prefix}:${queueName}:limiter:${workers[1].id}`;
      const globalKey = `${prefix}:${queueName}:limiter`;

      // each worker has its own key
      expect(await connection.exists(key0)).toBe(1);
      expect(await connection.exists(key1)).toBe(1);
      // global key must stay untouched
      expect(await connection.exists(globalKey)).toBe(0);

      await Promise.all(workers.map(w => w.close()));
    });
  });

  describe('manual rate-limit via RateLimitError', () => {
    it(
      'should use the per-worker key when a job throws RateLimitError',
      { timeout: 5000 },
      async () => {
        let attempts = 0;
        const worker = new Worker(
          queueName,
          async (job): Promise<void> => {
            attempts++;
            if (attempts === 1) {
              // first attempt: signal manual rate-limit
              await worker.rateLimit(500);
              throw new RateLimitError();
            }
            // second attempt: succeed
          },
          {
            connection,
            prefix,
            localLimiter: { max: 5, duration: 1000 },
          },
        );
        await worker.waitUntilReady();

        const completed = new Promise<void>((resolve, reject) => {
          queueEvents.on('completed', () => resolve());
          queueEvents.on('failed', async err => {
            await worker.close();
            reject(err);
          });
        });

        await queue.add('j', {});
        await completed;

        // after completing, global key must still be untouched
        const globalKey = `${prefix}:${queueName}:limiter`;
        expect(await connection.exists(globalKey)).toBe(0);

        await worker.close();
      },
    );
  });

  describe('validation', () => {
    it('should throw when both limiter and localLimiter are provided', () => {
      expect(
        () =>
          new Worker(queueName, async () => {}, {
            connection,
            prefix,
            limiter: { max: 5, duration: 1000 },
            localLimiter: { max: 5, duration: 1000 },
          }),
      ).toThrow('limiter and localLimiter cannot be used together');
    });
  });
});
