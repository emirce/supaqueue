import { getNextRun } from "./cron.js";
import type {
  Job,
  JobScheduler,
  RepeatStrategy,
  SchedulerOptions,
} from "./interface.js";

type SchedulerId = string;

type AddJob<TData> = (
  name: string,
  data: TData,
  options?: SchedulerOptions<TData>["jobOptions"],
) => Job<TData>;

type SchedulerInternal<TData> = JobScheduler<TData> & {
  timer: NodeJS.Timeout;
};

interface JobSchedulerManagerOptions<TData> {
  addJob: AddJob<TData>;
  now?: () => Date;
}

export class JobSchedulerManager<TData = unknown> {
  private readonly _addJob: AddJob<TData>;
  private readonly _now: () => Date;
  private readonly _schedulers: Map<SchedulerId, SchedulerInternal<TData>> =
    new Map();

  constructor(options: JobSchedulerManagerOptions<TData>) {
    this._addJob = options.addJob;
    this._now = options.now ?? (() => new Date());
  }

  upsert<TRepeatStrategy extends RepeatStrategy>(
    schedulerId: string,
    options: SchedulerOptions<TData, TRepeatStrategy>,
  ): JobScheduler<TData> {
    this.remove(schedulerId);

    const nextRunAt = this._getNextRunAt(options.repeat);
    const scheduler: SchedulerInternal<TData> = {
      id: schedulerId,
      name: options.name,
      data: options.data,
      repeat: options.repeat,
      nextRunAt,
      timer: this._scheduleNextTick(schedulerId, options, nextRunAt),
      ...(options.jobOptions === undefined
        ? {}
        : { jobOptions: options.jobOptions }),
    };

    this._schedulers.set(schedulerId, scheduler);
    return this._toPublicScheduler(scheduler);
  }

  remove(schedulerId: string): void {
    const scheduler = this._schedulers.get(schedulerId);
    if (!scheduler) return;

    clearTimeout(scheduler.timer);
    this._schedulers.delete(schedulerId);
  }

  get(schedulerId: string): JobScheduler<TData> | undefined {
    const scheduler = this._schedulers.get(schedulerId);
    if (!scheduler) return undefined;

    return this._toPublicScheduler(scheduler);
  }

  getAll(): JobScheduler<TData>[] {
    return [...this._schedulers.values()].map((scheduler) =>
      this._toPublicScheduler(scheduler),
    );
  }

  clear(): void {
    for (const scheduler of this._schedulers.values()) {
      clearTimeout(scheduler.timer);
    }
    this._schedulers.clear();
  }

  private _scheduleNextTick<TRepeatStrategy extends RepeatStrategy>(
    schedulerId: string,
    options: SchedulerOptions<TData, TRepeatStrategy>,
    runAt: Date,
  ): NodeJS.Timeout {
    const delay = Math.max(runAt.getTime() - this._now().getTime(), 0);

    return setTimeout(() => {
      const scheduler = this._schedulers.get(schedulerId);
      if (!scheduler) return;

      this._addJob(options.name, options.data, options.jobOptions);

      const nextRunAt = this._getNextRunAt(options.repeat);
      scheduler.nextRunAt = nextRunAt;
      scheduler.timer = this._scheduleNextTick(schedulerId, options, nextRunAt);
    }, delay);
  }

  private _getNextRunAt(
    repeat: SchedulerOptions<TData>["repeat"],
    from = this._now(),
  ): Date {
    if (repeat.strategy === "interval") {
      if (!Number.isFinite(repeat.interval) || repeat.interval <= 0) {
        throw new Error("Scheduler interval must be greater than zero");
      }

      return new Date(from.getTime() + repeat.interval);
    }

    return getNextRun(repeat.cron, from);
  }

  private _toPublicScheduler(
    scheduler: SchedulerInternal<TData>,
  ): JobScheduler<TData> {
    const { timer, ...publicScheduler } = scheduler;
    return publicScheduler;
  }
}
