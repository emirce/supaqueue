import { EventEmitter } from "node:events";
import {
  type Job,
  type JobOptions,
  JobRetryStrategy,
  type JobScheduler,
  JobStatus,
  type Processor,
  type Queue,
  type QueueListener,
  type QueueOptions,
  type RemoveOnJobs,
  type RepeatStrategy,
  type SchedulerOptions,
} from "./interface.js";
import { JobSchedulerManager } from "./scheduler.js";

type JobId = string;

type JobInternal<TData = unknown> = Job<TData> & {
  removed?: boolean;
};

interface FinishedJobMetadata {
  finishedAt: number;
  finishOrder: number;
}

const DEFAULT_CONCURRENCY = 1;

export const DEFAULT_JOB_OPTIONS: JobOptions = {
  delay: 0,
  attempts: 0,
  retryStrategy: JobRetryStrategy.FixedDelay,
  removeOnComplete: false,
  removeOnFail: false,
};

class QueueImpl<TData = unknown, TResult = unknown>
  extends EventEmitter<QueueListener<TData, TResult>>
  implements Queue<TData, TResult>
{
  private readonly _jobsMap: Map<JobId, JobInternal<TData>> = new Map();
  private readonly _finishedJobsMeta: Map<JobId, FinishedJobMetadata> =
    new Map();
  private readonly _timers: Map<JobId, NodeJS.Timeout> = new Map();
  private readonly _waitingJobs: JobId[] = [];
  private readonly _processorFn: Processor<TData, TResult>;
  private readonly _schedulerManager: JobSchedulerManager<TData>;
  private readonly _defaultJobOptions: JobOptions;

  private _concurrency: number;
  private _lifo: boolean;
  private _paused: boolean;
  private _activeJobCount: number = 0;
  private _finishOrder: number = 0;

  constructor(processorFn: Processor<TData, TResult>, options?: QueueOptions) {
    super();
    this._processorFn = processorFn;
    this._concurrency = options?.concurrency ?? DEFAULT_CONCURRENCY;
    this._lifo = options?.lifo ?? false;
    this._paused = options?.paused ?? false;
    this._defaultJobOptions = options?.defaultJobOptions ?? {};
    this._schedulerManager = new JobSchedulerManager<TData>({
      addJob: this.addJob.bind(this),
    });
  }
  upsertJobScheduler<TRepeatStrategy extends RepeatStrategy>(
    schedulerId: string,
    opts: SchedulerOptions<TData, TRepeatStrategy>,
  ): JobScheduler<TData> {
    return this._schedulerManager.upsert(schedulerId, opts);
  }
  removeJobScheduler(schedulerId: string): void {
    this._schedulerManager.remove(schedulerId);
  }
  getJobScheduler(schedulerId: string): JobScheduler<TData> | undefined {
    return this._schedulerManager.get(schedulerId);
  }
  getJobSchedulers(): JobScheduler<TData>[] {
    return this._schedulerManager.getAll();
  }
  getJobs(): Job<TData>[] {
    const jobs: Job<TData>[] = [];
    for (const job of this._jobsMap.values()) {
      if (!job.removed) {
        const { removed, ...jobData } = job;
        jobs.push(jobData);
      }
    }
    return jobs;
  }
  removeJob(jobId: string): void {
    const job = this._jobsMap.get(jobId);
    if (
      !job ||
      job.removed ||
      (job.status !== JobStatus.Waiting && job.status !== JobStatus.Delayed)
    )
      return;
    const timer = this._timers.get(jobId);
    if (timer) {
      clearTimeout(timer);
      this._timers.delete(jobId);
    }
    job.removed = true;
  }
  getJob(jobId: string): Job<TData> | undefined {
    const job = this._jobsMap.get(jobId);
    if (!job || job.removed) return undefined;
    const { removed, ...rest } = job;
    return rest;
  }

  getActiveJobCount(): number {
    return this._activeJobCount;
  }
  clear(): void {
    this._schedulerManager.clear();
    for (const timer of this._timers.values()) {
      clearTimeout(timer);
    }
    this._timers.clear();
    this._jobsMap.clear();
    this._finishedJobsMeta.clear();
    this._waitingJobs.length = 0;
  }
  pause(): void {
    if (this._paused) return;
    this._paused = true;
    this.emit("paused");
  }
  resume(): void {
    if (!this._paused) return;
    this._paused = false;
    this.emit("resumed");
    this._flushJobs();
  }
  isPaused(): boolean {
    return this._paused;
  }

  addJob(name: string, data: TData, options?: JobOptions): Job<TData> {
    const job = this._createJob(name, data, options);
    this._jobsMap.set(job.id, job);
    const addedJob = this._toPublicJob(job);

    if (job.status === JobStatus.Delayed) {
      this._scheduleDelayedJob(job);
      return addedJob;
    }

    this._enqueueJob(job);

    return addedJob;
  }
  private _scheduleDelayedJob(
    job: JobInternal<TData>,
    delay = job.options?.delay ?? 0,
  ) {
    if (delay <= 0) {
      this._enqueueJob(job);
      return;
    }
    const existingTimer = this._timers.get(job.id);
    if (existingTimer) {
      clearTimeout(existingTimer);
    }
    job.status = JobStatus.Delayed;
    const timer = setTimeout(() => {
      this._timers.delete(job.id);
      if (job.removed || !this._jobsMap.has(job.id)) return;
      this._enqueueJob(job);
    }, delay);
    this._timers.set(job.id, timer);
  }

  private _enqueueJob(job: JobInternal<TData>) {
    if (job.removed || !this._jobsMap.has(job.id)) return;
    job.status = JobStatus.Waiting;
    this._waitingJobs.push(job.id);
    this.emit("waiting", job);
    this._flushJobs();
  }

  private _flushJobs() {
    if (this._paused) return;
    while (this._activeJobCount < this._concurrency) {
      const job = this._consumeNextJob();
      if (!job) return;
      if (job.removed) {
        this._jobsMap.delete(job.id);
        continue;
      }
      this._activeJobCount++;
      job.status = JobStatus.Active;
      delete job.failedReason;
      Promise.resolve()
        .then(() => this._processorFn(job))
        .then((result) => this._handleJobCompletion(job, result))
        .catch((error) => this._handleJobFailure(job, error));
      this.emit("active", job);
    }
  }

  private _handleJobCompletion(job: JobInternal<TData>, result: TResult) {
    job.status = JobStatus.Completed;
    this._markJobFinished(job);
    this._activeJobCount--;
    this.emit("completed", job, result);
    this._applyFinishedJobRetention(job, job.options?.removeOnComplete);
    this._flushJobs();
  }

  private _handleJobFailure(job: JobInternal<TData>, error: Error) {
    this._activeJobCount--;
    job.attemptsMade++;
    job.failedReason = error.message;
    if (job.attemptsMade <= (job.options?.attempts ?? 0)) {
      this._retryJob(job);
      this._flushJobs();
      return;
    }
    job.status = JobStatus.Failed;
    this._markJobFinished(job);
    this.emit("failed", job, error);
    this._applyFinishedJobRetention(job, job.options?.removeOnFail);
    this._flushJobs();
  }

  private _markJobFinished(job: JobInternal<TData>) {
    this._finishedJobsMeta.set(job.id, {
      finishedAt: Date.now(),
      finishOrder: this._finishOrder++,
    });
  }

  private _applyFinishedJobRetention(
    job: JobInternal<TData>,
    removeOnJobs: RemoveOnJobs | undefined,
  ) {
    if (!removeOnJobs) return;

    if (removeOnJobs === true) {
      this._removeJobFromMemory(job.id);
      return;
    }

    const jobsWithSameStatus = this._getFinishedJobsByStatus(job.status);

    if (typeof removeOnJobs === "number") {
      this._pruneByCount(jobsWithSameStatus, removeOnJobs);
      return;
    }

    if (removeOnJobs.age !== undefined) {
      this._pruneByAge(jobsWithSameStatus, removeOnJobs.age);
    }

    if (removeOnJobs.count !== undefined) {
      this._pruneByCount(
        this._getFinishedJobsByStatus(job.status),
        removeOnJobs.count,
      );
    }
  }

  private _getFinishedJobsByStatus(status: JobStatus): JobInternal<TData>[] {
    return [...this._jobsMap.values()]
      .filter((job) => {
        return (
          !job.removed &&
          job.status === status &&
          this._finishedJobsMeta.has(job.id)
        );
      })
      .sort((a, b) => {
        const aMetadata = this._finishedJobsMeta.get(a.id);
        const bMetadata = this._finishedJobsMeta.get(b.id);
        return (aMetadata?.finishOrder ?? 0) - (bMetadata?.finishOrder ?? 0);
      });
  }

  private _pruneByAge(jobs: JobInternal<TData>[], ageInSeconds: number) {
    const cutoff = Date.now() - ageInSeconds * 1000;

    for (const job of jobs) {
      const metadata = this._finishedJobsMeta.get(job.id);
      if (!metadata || metadata.finishedAt > cutoff) continue;
      this._removeJobFromMemory(job.id);
    }
  }

  private _pruneByCount(jobs: JobInternal<TData>[], count: number) {
    const jobsToRemove = jobs.length - Math.max(Math.floor(count), 0);
    if (jobsToRemove <= 0) return;

    for (const job of jobs.slice(0, jobsToRemove)) {
      this._removeJobFromMemory(job.id);
    }
  }

  private _removeJobFromMemory(jobId: JobId) {
    const timer = this._timers.get(jobId);
    if (timer) {
      clearTimeout(timer);
      this._timers.delete(jobId);
    }
    this._jobsMap.delete(jobId);
    this._finishedJobsMeta.delete(jobId);
  }

  private _retryJob(job: JobInternal<TData>) {
    const delay = this._getRetryDelay(job);
    if (delay > 0) {
      this._scheduleDelayedJob(job, delay);
      return;
    }
    this._enqueueJob(job);
  }

  private _getRetryDelay(job: JobInternal<TData>) {
    const delay = job.options?.delay ?? 0;
    if (job.options?.retryStrategy !== JobRetryStrategy.ExponentialBackoff) {
      return delay;
    }
    return delay * 2 ** Math.max(job.attemptsMade - 1, 0);
  }

  private _consumeNextJob(): JobInternal<TData> | undefined {
    if (this._waitingJobs.length === 0) return undefined;
    const jobId = this._lifo
      ? this._waitingJobs.pop()
      : this._waitingJobs.shift();
    if (!jobId) return undefined;
    return this._jobsMap.get(jobId);
  }

  private _generateJobId(): JobId {
    const id = crypto.randomUUID();
    return id;
  }

  private _toPublicJob(job: JobInternal<TData>): Job<TData> {
    const { removed, ...rest } = job;
    return rest;
  }

  private _createJob(
    name: string,
    data: TData,
    options?: JobOptions,
  ): JobInternal<TData> {
    const jobId = this._generateJobId();
    const job: Job<TData> = {
      id: jobId,
      name,
      data,
      status:
        options?.delay && options.delay > 0
          ? JobStatus.Delayed
          : JobStatus.Waiting,
      attemptsMade: 0,
      options: {
        ...DEFAULT_JOB_OPTIONS,
        ...this._defaultJobOptions,
        ...options,
      },
    };
    return job;
  }
}

export const createQueue = <TData = unknown, TResult = unknown>(
  processor: Processor<TData, TResult>,
  options?: QueueOptions,
): Queue<TData, TResult> => {
  return new QueueImpl<TData, TResult>(processor, options);
};
