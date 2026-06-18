# supaqueue

`supaqueue` is a lightweight, type-safe in-memory background job queue for Node.js with zero dependencies. Use it when
you need background **jobs, retries, concurrency, delayed work, or scheduled
tasks** without setting up Redis, Postgres, or external queue infrastructure.

It is best suited for small apps, side projects, local tools, CLIs, and services
where losing queued jobs on process restart is acceptable. If you need durable
jobs across deploys or multiple workers across machines, use a persistent queue
such as BullMQ, pg-boss, or a hosted worker system.

## Features

- In-memory jobs with zero external services.
- Concurrency control, delayed jobs, and retries.
- Fixed-delay and exponential-backoff retry strategies.
- Pause/resume controls and job lifecycle events.
- Repeating jobs with intervals or cron expressions.
- TypeScript types for job data and results.

## Install

```sh
pnpm add supaqueue
```

```sh
npm install supaqueue
```

## Quick Start

```ts
import { createQueue } from "supaqueue";

const emailQueue = createQueue<{ to: string; subject: string }>(
  async (job) => {
    await sendEmail(job.data.to, job.data.subject);
    return { sent: true };
  },
  {
    concurrency: 2,
  },
);

emailQueue.addJob("welcome-email", {
  to: "dev@example.com",
  subject: "Welcome",
});
```

Jobs start processing as soon as they are added unless the queue is paused or
the job has a delay.

## Queue Options

```ts
const queue = createQueue(processor, {
  concurrency: 4,
  paused: false,
  lifo: false,
});
```

- `concurrency`: number of jobs to process at the same time. Defaults to `1`.
- `paused`: create the queue in a paused state. Defaults to `false`.
- `lifo`: process newest waiting jobs first. Defaults to FIFO.

## Adding Jobs

```ts
queue.addJob("resize-image", {
  imageId: "img_123",
});
```

### Delayed Jobs

```ts
queue.addJob(
  "send-reminder",
  { userId: "user_123" },
  { delay: 60_000 },
);
```

### Retries

```ts
import { JobRetryStrategy } from "supaqueue";

queue.addJob(
  "sync-account",
  { accountId: "acct_123" },
  {
    attempts: 3,
    delay: 1_000,
    retryStrategy: JobRetryStrategy.ExponentialBackoff,
  },
);
```

`attempts` is the number of retries after the first failed run. With fixed delay,
each retry waits for `delay`. With exponential backoff, the delay increases after
each failed attempt.

## Events

```ts
queue.on("waiting", (job) => {
  console.log("Waiting:", job.name);
});

queue.on("active", (job) => {
  console.log("Started:", job.id);
});

queue.on("completed", (job, result) => {
  console.log("Completed:", job.id, result);
});

queue.on("failed", (job, error) => {
  console.error("Failed:", job.id, error);
});
```

Available events:

- `waiting`
- `active`
- `completed`
- `failed`
- `paused`
- `resumed`

## Pause and Resume

```ts
queue.pause();

queue.addJob("queued-for-later", { id: 1 });

queue.resume();
```

Paused queues keep accepting jobs, but they do not process waiting jobs until
`resume()` is called.

## Inspecting and Removing Jobs

```ts
const job = queue.addJob("cleanup", { path: "/tmp/report.csv" });

queue.getJob(job.id);
queue.getJobs();
queue.getActiveJobCount();

queue.removeJob(job.id);
queue.clear();
```

`removeJob()` only removes jobs that are waiting or delayed. `clear()` removes
all jobs and schedulers and clears pending timers.

## Scheduled Jobs

Use schedulers for repeated background work. Schedulers add jobs to the queue on
an interval or cron expression.

### Interval Scheduler

```ts
queue.upsertJobScheduler("heartbeat", {
  name: "heartbeat",
  data: { service: "api" },
  repeat: {
    strategy: "interval",
    interval: 5_000,
  },
});
```

### Cron Scheduler

```ts
queue.upsertJobScheduler("daily-report", {
  name: "daily-report",
  data: { report: "usage" },
  repeat: {
    strategy: "cron",
    cron: "0 9 * * *",
  },
});
```

Cron expressions use five fields:

```txt
minute hour day-of-month month day-of-week
```

Supported cron syntax includes wildcards, ranges, lists, and steps, such as:

- `* * * * *`
- `*/5 * * * *`
- `0 9 * * 1-5`
- `0,30 * * * *`

Manage schedulers with:

```ts
queue.getJobScheduler("daily-report");
queue.getJobSchedulers();
queue.removeJobScheduler("daily-report");
```

## TypeScript

`supaqueue` is written in TypeScript and lets you type both job data and processor
results:

```ts
type JobData = { userId: string };
type JobResult = { ok: boolean };

const queue = createQueue<JobData, JobResult>(async (job) => {
  return { ok: job.data.userId.length > 0 };
});
```

## Notes

- Jobs live in memory only.
- Jobs are not shared across processes.
- Jobs are lost when the process exits.
- Scheduled jobs use Node.js timers.
