import { describe, expect, it } from "vitest";
import {
  CronParseError,
  getNextRun,
  getNextRuns,
  parseCronExpression,
} from "../src/cron";

describe("Cron parser", () => {
  it("should parse wildcard fields", () => {
    expect(parseCronExpression("* * * * *")).toEqual({
      expression: "* * * * *",
      minute: Array.from({ length: 60 }, (_, index) => index),
      hour: Array.from({ length: 24 }, (_, index) => index),
      dayOfMonth: Array.from({ length: 31 }, (_, index) => index + 1),
      month: Array.from({ length: 12 }, (_, index) => index + 1),
      dayOfWeek: [0, 1, 2, 3, 4, 5, 6],
    });
  });

  it("should parse exact values, lists, ranges, and steps", () => {
    expect(parseCronExpression("*/15 9-17/2 1,15 1-6 1,3,5")).toEqual({
      expression: "*/15 9-17/2 1,15 1-6 1,3,5",
      minute: [0, 15, 30, 45],
      hour: [9, 11, 13, 15, 17],
      dayOfMonth: [1, 15],
      month: [1, 2, 3, 4, 5, 6],
      dayOfWeek: [1, 3, 5],
    });
  });

  it("should normalize Sunday from 7 to 0", () => {
    expect(parseCronExpression("0 0 * * 0,7").dayOfWeek).toEqual([0]);
  });

  it("should reject invalid expressions", () => {
    const invalidExpressions = [
      "",
      "* * * *",
      "* * * * * *",
      "60 * * * *",
      "* 24 * * *",
      "* * 0 * *",
      "* * * 13 *",
      "* * * * 8",
      "*/0 * * * *",
      "5/2 * * * *",
      "10-5 * * * *",
      "1,,2 * * * *",
      "a * * * *",
    ];

    for (const expression of invalidExpressions) {
      expect(() => parseCronExpression(expression), expression).toThrow(
        CronParseError,
      );
    }
  });

  it("should find the next minutely run strictly after the provided date", () => {
    expect(
      getNextRun("* * * * *", new Date(2026, 0, 1, 0, 0, 0)).toISOString(),
    ).toBe(new Date(2026, 0, 1, 0, 1, 0).toISOString());
  });

  it("should find the next hourly run", () => {
    expect(
      getNextRun("0 * * * *", new Date(2026, 0, 1, 8, 30, 0)).toISOString(),
    ).toBe(new Date(2026, 0, 1, 9, 0, 0).toISOString());
  });

  it("should find the next daily run", () => {
    expect(
      getNextRun("30 9 * * *", new Date(2026, 0, 1, 9, 30, 0)).toISOString(),
    ).toBe(new Date(2026, 0, 2, 9, 30, 0).toISOString());
  });

  it("should find the next weekly run", () => {
    expect(
      getNextRun("0 12 * * 1", new Date(2026, 0, 2, 13, 0, 0)).toISOString(),
    ).toBe(new Date(2026, 0, 5, 12, 0, 0).toISOString());
  });

  it("should find the next monthly run", () => {
    expect(
      getNextRun("0 8 15 * *", new Date(2026, 0, 15, 8, 0, 0)).toISOString(),
    ).toBe(new Date(2026, 1, 15, 8, 0, 0).toISOString());
  });

  it("should roll over hour, day, and month boundaries", () => {
    expect(
      getNextRun("0 0 1 2 *", new Date(2026, 0, 31, 23, 59, 0)).toISOString(),
    ).toBe(new Date(2026, 1, 1, 0, 0, 0).toISOString());
  });

  it("should use classic OR matching for day-of-month and day-of-week", () => {
    expect(
      getNextRun("0 9 15 * 1", new Date(2026, 0, 1, 0, 0, 0)).toISOString(),
    ).toBe(new Date(2026, 0, 5, 9, 0, 0).toISOString());
  });

  it("should return ordered repeated run dates", () => {
    expect(
      getNextRuns("*/20 * * * *", 4, new Date(2026, 0, 1, 0, 0, 0)),
    ).toEqual([
      new Date(2026, 0, 1, 0, 20, 0),
      new Date(2026, 0, 1, 0, 40, 0),
      new Date(2026, 0, 1, 1, 0, 0),
      new Date(2026, 0, 1, 1, 20, 0),
    ]);
  });

  it("should reject invalid next run counts", () => {
    expect(() => getNextRuns("* * * * *", -1)).toThrow(CronParseError);
    expect(() => getNextRuns("* * * * *", 1.5)).toThrow(CronParseError);
  });
});
