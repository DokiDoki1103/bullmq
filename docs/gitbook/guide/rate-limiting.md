# Rate limiting

BullMQ provides queue rate limiting. It is possible to configure workers so that they obey a given rate limiting option:

```typescript
import { Worker, QueueScheduler } from 'bullmq';

const worker = new Worker('painter', async job => paintCar(job), {
  limiter: {
    max: 10,
    duration: 1000,
  },
});

const scheduler = new QueueScheduler('painter');
```

{% hint style="warning" %}
Jobs that get rate limited will actually stay in the waiting state.
{% endhint %}

{% hint style="danger" %}
From BullMQ 2.0 and onwards, the `QueueScheduler` is not needed anymore.
{% endhint %}

{% hint style="info" %}
The rate limiter is global, so if you have for example 10 workers for one queue with the above settings, still only 10 jobs will be processed by second.
{% endhint %}

### Per-worker (local) rate limiter

If you want each worker instance to have its **own independent** rate limit — so that N workers yield approximately N× total throughput — use `localLimiter` instead of `limiter`:

```typescript
import { Worker } from 'bullmq';

// 5 pods × 10 jobs/s each = ~50 jobs/s total
const worker = new Worker('painter', async job => paintCar(job), {
  localLimiter: {
    max: 10,
    duration: 1000,
  },
});
```

Each worker maintains its own Redis counter (`{prefix}:{queue}:limiter:{workerId}`), so adding more workers scales total throughput proportionally. The key is cleaned up automatically when the worker calls `close()`.

{% hint style="warning" %}
`limiter` and `localLimiter` cannot be used on the same worker instance. They are mutually exclusive.
{% endhint %}

{% hint style="warning" %}
For manual rate-limiting with `localLimiter`, use `worker.rateLimit()` (not `queue.rateLimit()`). `queue.rateLimit()` writes to the shared global key and has no effect on per-worker limits.
{% endhint %}

### Group keys

{% hint style="danger" %}
From BullMQ 3.0 and onwards, group keys support is removed to improve global rate limit, so the information below is only valid for older versions.
{% endhint %}

It is also possible to define a rate limiter based on group keys, for example you may want to have a rate limiter per _customer_ instead of a global rate limiter for all customers:

```typescript
import { Queue, Worker, QueueScheduler } from 'bullmq';

const queue = new Queue('painter', {
  limiter: {
    groupKey: 'customerId',
  },
});

const worker = new Worker('painter', async job => paintCar(job), {
  limiter: {
    max: 10,
    duration: 1000,
    groupKey: 'customerId',
  },
});

const scheduler = new QueueScheduler('painter');

// jobs will be rate limited by the value of customerId key:
await queue.add('rate limited paint', { customerId: 'my-customer-id' });
```

### Manual rate-limit

Sometimes is useful to rate-limit a queue manually instead of based on some static options. For example, you may have an API that returns `429 Too Many Requests`, and you want to rate-limit the queue based on that response.

For this purpose, you can use the worker method **`rateLimit`** like this:

```typescript
import { Worker } from 'bullmq';

const worker = new Worker(
  'myQueue',
  async () => {
    const [isRateLimited, duration] = await doExternalCall();
    if (isRateLimited) {
      await worker.rateLimit(duration);
      // Do not forget to throw this special exception,
      // since we must differentiate this case from a failure
      // in order to move the job to wait again.
      throw Worker.RateLimitError();
    }
  },
  {
    connection,
    limiter: {
      max: 1,
      duration: 500,
    },
  },
);
```

{% hint style="warning" %}
Don't forget to pass limiter options into your worker's options as _limiter.max_ is used to determine if we need to execute the rate limit validation.
{% endhint %}

### Get Queue Rate Limit Ttl

Sometimes is useful to know if our queue is rate limited.

For this purpose, you can use the **`getRateLimitTtl`** method like this:

```typescript
import { Queue } from 'bullmq';

const queue = new Queue('myQueue', { connection });
const maxJobs = 100;

const ttl = await queue.getRateLimitTtl(maxJobs);

if (ttl > 0) {
  console.log('Queue is rate limited');
}
```

### Remove Rate Limit Key

Sometimes is useful to stop a rate limit delay.

For this purpose, you can use the **`removeRateLimitKey`** method like this:

```typescript
import { Queue } from 'bullmq';

const queue = new Queue('myQueue', { connection });

await queue.removeRateLimitKey();
```

By removing rate limit key, workers will be able to pick jobs again and your rate limit counter is reset to zero.

## Read more:

- 💡 [Rate Limit API Reference](https://docs.bullmq.io/api/classes/v6.Worker.html#ratelimit)
- 💡 [Get Rate Limit Ttl API Reference](https://docs.bullmq.io/api/classes/v6.Queue.html#getratelimitttl)
- 💡 [Remove Rate Limit Key API Reference](https://docs.bullmq.io/api/classes/v6.Queue.html#removeratelimitkey)
