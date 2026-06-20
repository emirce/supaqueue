import { describe, expect, it, vi } from "vitest";
import { type Job, JobRetryStrategy, JobStatus } from "../src/interface";
import { createQueue, DEFAULT_JOB_OPTIONS } from "../src/queue";

describe("Queue", () => {
  it("should create add a simple job to the queue and return it", () => {
    const queue = createQueue(async (_job) => {
      return "Hello, World!";
    });

    const job = queue.addJob("greet", { name: "Alice" });

    expect(job).toHaveProperty("id");
    expect(job).toHaveProperty("name", "greet");
    expect(job).toHaveProperty("status", JobStatus.Waiting);
    expect(job).toHaveProperty("data", { name: "Alice" });
    expect(job).toHaveProperty("options", DEFAULT_JOB_OPTIONS);
  });

  it("should run a job after being added", async () => {
    let resolve: (value: unknown) => void;
    const promise = new Promise((res) => {
      resolve = res;
    });

    const queue = createQueue<{ name: string }>(async (job) => {
      resolve(job.data);
    });
    queue.addJob("greet", { name: "Alice" });

    const result = await promise;
    expect(result).toEqual({ name: "Alice" });
  });

  it("should run multiple jobs after being added", async () => {
    let resolve: (value: unknown) => void;
    const processed: { name: string }[] = [];
    const promise = new Promise((res) => {
      resolve = res;
    });
    const jobCount = 5;

    const queue = createQueue<{ name: string }>(async (job) => {
      processed.push(job.data);
      if (processed.length === jobCount) {
        resolve(processed);
      }
    });

    for (let i = 0; i < jobCount; i++) {
      queue.addJob("greet", { name: `Alice ${i}` });
    }

    const result = await promise;
    expect(result).toHaveLength(5);
    expect(result).toEqual([
      { name: "Alice 0" },
      { name: "Alice 1" },
      { name: "Alice 2" },
      { name: "Alice 3" },
      { name: "Alice 4" },
    ]);
  });

  it("should not run more jobs than the concurrency limit", async () => {
    const processor = vi.fn(async (_job: Job<{ name: string }>) => {
      return new Promise((res) => setTimeout(res, 100));
    });

    const queue = createQueue<{ name: string }>(processor, {
      concurrency: 5,
    });

    for (let i = 0; i < 10; i++) {
      queue.addJob("greet", { name: `Alice ${i}` });
    }

    await new Promise((res) => setTimeout(res, 50));
    expect(queue.getActiveJobCount()).toBe(5);
    expect(processor).toHaveBeenCalledTimes(5);

    await new Promise((res) => setTimeout(res, 100));
    expect(queue.getActiveJobCount()).toBe(5);
    expect(processor).toHaveBeenCalledTimes(10);
  });

  it("should expose persisted job status changes", async () => {
    let resolveProcessor: (value: string) => void;
    const queue = createQueue<{ name: string }, string>(() => {
      return new Promise((resolve) => {
        resolveProcessor = resolve;
      });
    });
    const completed = new Promise((resolve) => {
      queue.on("completed", resolve);
    });

    const job = queue.addJob("greet", { name: "Alice" });
    expect(job.status).toBe(JobStatus.Waiting);
    expect(queue.getJob(job.id)?.status).toBe(JobStatus.Active);

    await Promise.resolve();
    if (!resolveProcessor) {
      throw new Error("processor did not start");
    }
    resolveProcessor("done");
    await completed;

    expect(queue.getJob(job.id)?.status).toBe(JobStatus.Completed);
  });

  it("should start jobs up to the concurrency limit when resumed", async () => {
    const processor = vi.fn(async () => new Promise(() => {}));
    const queue = createQueue<{ name: string }>(processor, {
      concurrency: 5,
      paused: true,
    });

    for (let i = 0; i < 10; i++) {
      queue.addJob("greet", { name: `Alice ${i}` });
    }

    await Promise.resolve();
    expect(processor).not.toHaveBeenCalled();

    queue.resume();
    await Promise.resolve();

    expect(queue.getActiveJobCount()).toBe(5);
    expect(processor).toHaveBeenCalledTimes(5);
  });

  it("should remove a job from a queue", async () => {
    const queue = createQueue(async (_job) => {
      return "Hello, World!";
    });
    queue.pause();

    const job = queue.addJob("greet", { name: "Alice" });

    expect(queue.getJob(job.id)).toEqual(job);

    queue.removeJob(job.id);

    const fetchedJob = queue.getJob(job.id);
    expect(fetchedJob).toBeUndefined();
  });

  it("should not run removed jobs", async () => {
    const processor = vi.fn(async (_job: Job<{ name: string }>) => {
      return new Promise((res) => setTimeout(res, 100));
    });
    const queue = createQueue(processor, {
      concurrency: 1,
    });
    queue.pause();

    const job1 = queue.addJob("greet", { name: "Alice" });
    const job2 = queue.addJob("greet", { name: "Bob" });

    queue.removeJob(job1.id);
    queue.resume();

    await new Promise((res) => setTimeout(res, 150));
    expect(processor).toHaveBeenCalledTimes(1);
    expect(processor).toHaveBeenCalledWith(
      expect.objectContaining({ id: job2.id }),
    );
  });

  it("should run a delayed job after the delay has passed", async () => {
    vi.useFakeTimers();

    try {
      const processed: { name: string }[] = [];
      const queue = createQueue<{ name: string }>(async (job) => {
        processed.push(job.data);
        return job.data;
      });

      queue.addJob("delayed", { name: "Alice" }, { delay: 5000 });
      await vi.advanceTimersByTimeAsync(1000);
      expect(processed).toEqual([]);
      await vi.advanceTimersByTimeAsync(4000);
      expect(processed).toEqual([{ name: "Alice" }]);
    } finally {
      vi.useRealTimers();
    }
  });

  it("should not run delayed jobs after clear", async () => {
    vi.useFakeTimers();

    try {
      const processor = vi.fn(async () => "done");
      const queue = createQueue(processor);

      queue.addJob("delayed", { name: "Alice" }, { delay: 5000 });
      queue.clear();
      await vi.advanceTimersByTimeAsync(5000);

      expect(processor).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("should retry failed jobs according to attempts", async () => {
    let calls = 0;
    const queue = createQueue(async () => {
      calls++;
      if (calls === 1) {
        throw new Error("try again");
      }
      return "done";
    });
    const failed = vi.fn();
    queue.on("failed", failed);
    const completed = new Promise((resolve) => {
      queue.on("completed", resolve);
    });

    const job = queue.addJob(
      "retry",
      {},
      {
        attempts: 1,
        retryStrategy: JobRetryStrategy.FixedDelay,
      },
    );
    await completed;

    expect(calls).toBe(2);
    expect(failed).not.toHaveBeenCalled();
    expect(queue.getJob(job.id)).toEqual(
      expect.objectContaining({
        attemptsMade: 1,
        status: JobStatus.Completed,
      }),
    );
  });

  it("should handle processors that throw synchronously", async () => {
    const queue = createQueue(() => {
      throw new Error("boom");
    });
    const failed = new Promise((resolve) => {
      queue.on("failed", resolve);
    });

    const job = queue.addJob("sync-error", {});
    await failed;

    expect(queue.getActiveJobCount()).toBe(0);
    expect(queue.getJob(job.id)).toEqual(
      expect.objectContaining({
        failedReason: "boom",
        status: JobStatus.Failed,
      }),
    );
  });

  it("should process jobs in LIFO order when enabled", async () => {
    const processed: string[] = [];
    const queue = createQueue<{ name: string }>(
      async (job) => {
        processed.push(job.data.name);
        return job.data.name;
      },
      {
        lifo: true,
        paused: true,
      },
    );
    const completed = new Promise((resolve) => {
      queue.on("completed", () => {
        if (processed.length === 3) {
          resolve(processed);
        }
      });
    });

    queue.addJob("greet", { name: "Alice" });
    queue.addJob("greet", { name: "Bob" });
    queue.addJob("greet", { name: "Charlie" });
    queue.resume();
    await completed;

    expect(processed).toEqual(["Charlie", "Bob", "Alice"]);
  });

  it("should fail after retry attempts are exhausted", async () => {
    const processor = vi.fn(async () => {
      throw new Error("still broken");
    });
    const queue = createQueue(processor);
    const failed = new Promise((resolve) => {
      queue.on("failed", resolve);
    });

    const job = queue.addJob("retry", {}, { attempts: 2 });
    await failed;

    expect(processor).toHaveBeenCalledTimes(3);
    expect(queue.getJob(job.id)).toEqual(
      expect.objectContaining({
        attemptsMade: 3,
        failedReason: "still broken",
        status: JobStatus.Failed,
      }),
    );
  });

  it("should use exponential backoff delay between retry attempts", async () => {
    vi.useFakeTimers();

    try {
      const processor = vi.fn(async () => {
        throw new Error("try again later");
      });
      const queue = createQueue(processor);

      queue.addJob(
        "retry",
        {},
        {
          attempts: 2,
          delay: 1000,
          retryStrategy: JobRetryStrategy.ExponentialBackoff,
        },
      );

      await vi.advanceTimersByTimeAsync(999);
      expect(processor).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(processor).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(999);
      expect(processor).toHaveBeenCalledTimes(1);

      await vi.advanceTimersByTimeAsync(1);
      expect(processor).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(1999);
      expect(processor).toHaveBeenCalledTimes(2);

      await vi.advanceTimersByTimeAsync(1);
      expect(processor).toHaveBeenCalledTimes(3);
    } finally {
      vi.useRealTimers();
    }
  });

  it("should remove delayed jobs before they run", async () => {
    vi.useFakeTimers();

    try {
      const processor = vi.fn(async () => "done");
      const queue = createQueue(processor);
      const job = queue.addJob("delayed", { name: "Alice" }, { delay: 5000 });

      queue.removeJob(job.id);
      await vi.advanceTimersByTimeAsync(5000);

      expect(queue.getJob(job.id)).toBeUndefined();
      expect(processor).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("should return visible jobs from getJobs", async () => {
    const queue = createQueue(async () => "done", { paused: true });
    const removedJob = queue.addJob("removed", {});
    const keptJob = queue.addJob("kept", {});

    queue.removeJob(removedJob.id);
    expect(queue.getJobs()).toEqual([keptJob]);

    const completed = new Promise((resolve) => {
      queue.on("completed", resolve);
    });
    queue.resume();
    await completed;

    expect(queue.getJobs()).toEqual([
      expect.objectContaining({
        id: keptJob.id,
        status: JobStatus.Completed,
      }),
    ]);
  });

  it("should remove completed jobs when removeOnComplete is true", async () => {
    const queue = createQueue(async () => "done");
    const completed = new Promise<Job>((resolve) => {
      queue.on("completed", resolve);
    });

    const job = queue.addJob("cleanup", {}, { removeOnComplete: true });
    const completedJob = await completed;

    expect(completedJob.id).toBe(job.id);
    expect(completedJob.status).toBe(JobStatus.Completed);
    expect(queue.getJob(job.id)).toBeUndefined();
    expect(queue.getJobs()).toEqual([]);
  });

  it("should remove failed jobs when removeOnFail is true", async () => {
    const queue = createQueue(() => {
      throw new Error("boom");
    });
    const failed = new Promise<Job>((resolve) => {
      queue.on("failed", resolve);
    });

    const job = queue.addJob("cleanup", {}, { removeOnFail: true });
    const failedJob = await failed;

    expect(failedJob.id).toBe(job.id);
    expect(failedJob.status).toBe(JobStatus.Failed);
    expect(queue.getJob(job.id)).toBeUndefined();
    expect(queue.getJobs()).toEqual([]);
  });

  it("should apply removeOnComplete after retries succeed", async () => {
    let calls = 0;
    const queue = createQueue(() => {
      calls++;
      if (calls === 1) {
        throw new Error("try again");
      }
      return "done";
    });
    const failed = vi.fn();
    queue.on("failed", failed);
    const completed = new Promise<Job>((resolve) => {
      queue.on("completed", resolve);
    });

    const job = queue.addJob(
      "retry",
      {},
      {
        attempts: 1,
        removeOnComplete: true,
        removeOnFail: true,
      },
    );
    await completed;

    expect(failed).not.toHaveBeenCalled();
    expect(queue.getJob(job.id)).toBeUndefined();
  });

  it("should apply removeOnFail after retry attempts are exhausted", async () => {
    const processor = vi.fn(() => {
      throw new Error("still broken");
    });
    const queue = createQueue(processor);
    const failed = new Promise<Job>((resolve) => {
      queue.on("failed", resolve);
    });

    const job = queue.addJob(
      "retry",
      {},
      {
        attempts: 2,
        removeOnFail: true,
      },
    );
    await failed;

    expect(processor).toHaveBeenCalledTimes(3);
    expect(queue.getJob(job.id)).toBeUndefined();
  });

  it("should keep the newest completed jobs when removeOnComplete is a count", async () => {
    const completedIds: string[] = [];
    const queue = createQueue(async () => "done");
    const completed = new Promise<void>((resolve) => {
      queue.on("completed", (job) => {
        completedIds.push(job.id);
        if (completedIds.length === 3) {
          resolve();
        }
      });
    });

    queue.addJob("first", {}, { removeOnComplete: 2 });
    queue.addJob("second", {}, { removeOnComplete: 2 });
    queue.addJob("third", {}, { removeOnComplete: 2 });
    await completed;

    expect(queue.getJobs()).toEqual([
      expect.objectContaining({ id: completedIds[1] }),
      expect.objectContaining({ id: completedIds[2] }),
    ]);
    expect(queue.getJob(completedIds[0])).toBeUndefined();
  });

  it("should keep the newest failed jobs when removeOnFail is a count", async () => {
    const failedIds: string[] = [];
    const queue = createQueue(() => {
      throw new Error("boom");
    });
    const failed = new Promise<void>((resolve) => {
      queue.on("failed", (job) => {
        failedIds.push(job.id);
        if (failedIds.length === 3) {
          resolve();
        }
      });
    });

    queue.addJob("first", {}, { removeOnFail: 2 });
    queue.addJob("second", {}, { removeOnFail: 2 });
    queue.addJob("third", {}, { removeOnFail: 2 });
    await failed;

    expect(queue.getJobs()).toEqual([
      expect.objectContaining({ id: failedIds[1] }),
      expect.objectContaining({ id: failedIds[2] }),
    ]);
    expect(queue.getJob(failedIds[0])).toBeUndefined();
  });

  it("should remove completed jobs older than removeOnComplete age", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 0, 1, 0, 0, 0));

    try {
      const completedIds: string[] = [];
      const queue = createQueue(async () => "done");
      queue.on("completed", (job) => {
        completedIds.push(job.id);
      });

      queue.addJob("first", {}, { removeOnComplete: { age: 1 } });
      await vi.advanceTimersByTimeAsync(500);
      queue.addJob("second", {}, { removeOnComplete: { age: 1 } });
      await vi.advanceTimersByTimeAsync(500);
      queue.addJob("third", {}, { removeOnComplete: { age: 1 } });
      await vi.advanceTimersByTimeAsync(0);

      expect(queue.getJob(completedIds[0])).toBeUndefined();
      expect(queue.getJob(completedIds[1])).toBeDefined();
      expect(queue.getJob(completedIds[2])).toBeDefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it("should apply queue default job retention options", async () => {
    const queue = createQueue(async () => "done", {
      defaultJobOptions: { removeOnComplete: true },
    });
    const completed = new Promise<Job>((resolve) => {
      queue.on("completed", resolve);
    });

    const job = queue.addJob("cleanup", {});
    await completed;

    expect(queue.getJob(job.id)).toBeUndefined();
  });

  it("should allow job retention options to override queue defaults", async () => {
    const queue = createQueue(async () => "done", {
      defaultJobOptions: { removeOnComplete: true },
    });
    const completed = new Promise<Job>((resolve) => {
      queue.on("completed", resolve);
    });

    const job = queue.addJob("keep", {}, { removeOnComplete: false });
    await completed;

    expect(queue.getJob(job.id)).toEqual(
      expect.objectContaining({ status: JobStatus.Completed }),
    );
  });

  it("should apply queue default job retention to scheduler-created jobs", async () => {
    vi.useFakeTimers();

    try {
      const queue = createQueue(async () => "done", {
        defaultJobOptions: { removeOnComplete: true },
      });
      const completed = new Promise<Job>((resolve) => {
        queue.on("completed", resolve);
      });

      queue.upsertJobScheduler("every-second", {
        name: "cleanup",
        data: {},
        repeat: { strategy: "interval", interval: 1000 },
      });

      await vi.advanceTimersByTimeAsync(1000);
      const job = await completed;

      expect(queue.getJob(job.id)).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it("should leave jobs waiting when concurrency is zero", async () => {
    const processor = vi.fn(async () => "done");
    const queue = createQueue(processor, { concurrency: 0 });

    const job = queue.addJob("stalled", {});
    await Promise.resolve();

    expect(processor).not.toHaveBeenCalled();
    expect(queue.getActiveJobCount()).toBe(0);
    expect(queue.getJob(job.id)?.status).toBe(JobStatus.Waiting);
  });

  it("should leave later jobs waiting when an active processor never settles", async () => {
    const processor = vi.fn(async () => new Promise(() => {}));
    const queue = createQueue(processor, { concurrency: 1 });

    const firstJob = queue.addJob("blocked", {});
    const secondJob = queue.addJob("waiting", {});
    await Promise.resolve();

    expect(processor).toHaveBeenCalledTimes(1);
    expect(queue.getActiveJobCount()).toBe(1);
    expect(queue.getJob(firstJob.id)?.status).toBe(JobStatus.Active);
    expect(queue.getJob(secondJob.id)?.status).toBe(JobStatus.Waiting);
  });

  it("should propagate waiting listener errors before dispatching", () => {
    const processor = vi.fn(async () => "done");
    const queue = createQueue(processor);
    queue.on("waiting", () => {
      throw new Error("listener failed");
    });

    expect(() => queue.addJob("listener-error", {})).toThrow("listener failed");
    expect(processor).not.toHaveBeenCalled();
    expect(queue.getJobs()).toEqual([
      expect.objectContaining({
        name: "listener-error",
        status: JobStatus.Waiting,
      }),
    ]);
  });

  it("should enqueue jobs from an interval scheduler", async () => {
    vi.useFakeTimers();

    try {
      const processor = vi.fn(async () => "done");
      const queue = createQueue<{ name: string }>(processor);

      const scheduler = queue.upsertJobScheduler("every-second", {
        name: "greet",
        data: { name: "Alice" },
        repeat: { strategy: "interval", interval: 1000 },
      });

      expect(queue.getJobScheduler("every-second")).toEqual(scheduler);
      expect(queue.getJobSchedulers()).toEqual([scheduler]);

      await vi.advanceTimersByTimeAsync(999);
      expect(processor).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(processor).toHaveBeenCalledTimes(1);
      expect(processor).toHaveBeenCalledWith(
        expect.objectContaining({
          name: "greet",
          data: { name: "Alice" },
        }),
      );

      await vi.advanceTimersByTimeAsync(1000);
      expect(processor).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it("should replace an existing job scheduler when upserted", async () => {
    vi.useFakeTimers();

    try {
      const processor = vi.fn(async () => "done");
      const queue = createQueue<{ name: string }>(processor);

      queue.upsertJobScheduler("replace-me", {
        name: "old",
        data: { name: "Alice" },
        repeat: { strategy: "interval", interval: 1000 },
      });
      queue.upsertJobScheduler("replace-me", {
        name: "new",
        data: { name: "Bob" },
        repeat: { strategy: "interval", interval: 5000 },
      });

      await vi.advanceTimersByTimeAsync(1000);
      expect(processor).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(4000);
      expect(processor).toHaveBeenCalledTimes(1);
      expect(processor).toHaveBeenCalledWith(
        expect.objectContaining({
          name: "new",
          data: { name: "Bob" },
        }),
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it("should stop enqueueing jobs after scheduler removal", async () => {
    vi.useFakeTimers();

    try {
      const processor = vi.fn(async () => "done");
      const queue = createQueue<{ name: string }>(processor);

      queue.upsertJobScheduler("temporary", {
        name: "greet",
        data: { name: "Alice" },
        repeat: { strategy: "interval", interval: 1000 },
      });
      queue.removeJobScheduler("temporary");

      await vi.advanceTimersByTimeAsync(1000);

      expect(queue.getJobScheduler("temporary")).toBeUndefined();
      expect(queue.getJobSchedulers()).toEqual([]);
      expect(processor).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("should stop scheduler timers when clearing the queue", async () => {
    vi.useFakeTimers();

    try {
      const processor = vi.fn(async () => "done");
      const queue = createQueue<{ name: string }>(processor);

      queue.upsertJobScheduler("clear-me", {
        name: "greet",
        data: { name: "Alice" },
        repeat: { strategy: "interval", interval: 1000 },
      });
      queue.clear();

      await vi.advanceTimersByTimeAsync(1000);

      expect(queue.getJobSchedulers()).toEqual([]);
      expect(processor).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("should enqueue jobs from a cron scheduler", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 0, 1, 0, 0, 0));

    try {
      const processor = vi.fn(async () => "done");
      const queue = createQueue<{ name: string }>(processor);

      const scheduler = queue.upsertJobScheduler("every-minute", {
        name: "greet",
        data: { name: "Alice" },
        repeat: { strategy: "cron", cron: "* * * * *" },
      });

      expect(scheduler.nextRunAt).toEqual(new Date(2026, 0, 1, 0, 1, 0));

      await vi.advanceTimersByTimeAsync(59999);
      expect(processor).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(processor).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
