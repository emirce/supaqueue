import { createQueue } from "../src/queue";

const someQueue = createQueue<{ message: string }>(
  (job) => {
    console.log(`Processing job ${job.name} with data:`, job.data.message);
  },
  {
    concurrency: 2,
  },
);

someQueue.upsertJobScheduler("scheduler-id", {
  name: "My Scheduled Job",
  data: { message: "Hello, World!" },
  repeat: {
    strategy: "interval",
    interval: 5000, // Run every 5 seconds
  },
});
