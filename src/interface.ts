import type { EventEmitter } from "node:events";

export enum JobStatus {
  Waiting,
  Active,
  Completed,
  Failed,
  Delayed,
}

export enum JobRetryStrategy {
  ExponentialBackoff,
  FixedDelay,
}

export interface JobOptions {
  delay?: number;
  attempts?: number;
  retryStrategy?: JobRetryStrategy;
}

export interface Job<TData = unknown> {
  id: string;
  name: string;
  status: JobStatus;
  data: TData;
  options?: JobOptions;
  failedReason?: string;
  delay?: number;
  attemptsMade: number;
}

export interface QueueOptions {
  paused?: boolean;
  concurrency?: number;
  lifo?: boolean;
}

export interface QueueListener<TData, TResult> {
  paused: [];
  resumed: [];
  waiting: [job: Job<TData>];
  active: [job: Job<TData>];
  completed: [job: Job<TData>, result: TResult];
  failed: [job: Job<TData>, error: Error];
  error: [error: Error];
}

export interface Queue<TData = unknown, TResult = unknown>
  extends EventEmitter<QueueListener<TData, TResult>> {
  addJob(name: string, data: TData, options?: JobOptions): Job<TData>;
  removeJob(jobId: string): void;
  getJob(jobId: string): Job<TData> | undefined;
  getJobs(): Job<TData>[];
  getActiveJobCount(): number;
  clear(): void;
  pause(): void;
  resume(): void;
  isPaused(): boolean;
}

export type Processor<TData = unknown, TResult = unknown> = (
  job: Job<TData>,
) => Promise<TResult>;
