import { createQueue } from "./queue";

type ExampleData = {
  message: string;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const exampleQueue = createQueue<ExampleData, string>(
  async (job) => {
    // sleep randomly between 1 and 10 seconds to simulate work
    const randomDelay = Math.floor(Math.random() * 2000) + 1000;
    await sleep(randomDelay);
    const msg =
      "I slept for " +
      randomDelay +
      " ms and my message is: " +
      job.data.message;
    if (Math.random() < 0.5) {
      return msg;
    }
    throw new Error("Example error");
  },
  {
    concurrency: 10,
  },
);

exampleQueue.on("completed", (job, result) => {
  console.log(`Job ${job.id} completed with result: ${result}`);
});

exampleQueue.on("failed", (job, error) => {
  console.error(`Job ${job.id} failed with error: ${error.message}`);
});

exampleQueue.on("waiting", (job) => {
  console.log(`Job ${job.id} is waiting to be processed`);
});

const main = async () => {
  for (let i = 0; i < 5; i++) {
    exampleQueue.addJob(
      "exampleJob-" + i,
      { message: `Hello ${i}` },
      { delay: 5000 },
    );
  }

  const loop = async () => {
    while (true) {
      await sleep(5000);
    }
  };

  await loop();
};

main().catch((error) => {
  console.error("Error in main:", error);
});
