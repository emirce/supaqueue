const CRON_FIELD_COUNT = 5;
const MAX_SEARCH_YEARS = 8;

type CronFieldName = "minute" | "hour" | "dayOfMonth" | "month" | "dayOfWeek";

interface CronFieldDefinition {
  name: CronFieldName;
  min: number;
  max: number;
  normalize?: (value: number) => number;
}

export interface CronSchedule {
  expression: string;
  minute: number[];
  hour: number[];
  dayOfMonth: number[];
  month: number[];
  dayOfWeek: number[];
}

export class CronParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CronParseError";
  }
}

const FIELD_DEFINITIONS: CronFieldDefinition[] = [
  { name: "minute", min: 0, max: 59 },
  { name: "hour", min: 0, max: 23 },
  { name: "dayOfMonth", min: 1, max: 31 },
  { name: "month", min: 1, max: 12 },
  {
    name: "dayOfWeek",
    min: 0,
    max: 7,
    normalize: (value) => (value === 7 ? 0 : value),
  },
];

export const parseCronExpression = (expression: string): CronSchedule => {
  const trimmedExpression = expression.trim();
  if (!trimmedExpression) {
    throw new CronParseError("Cron expression cannot be empty");
  }

  const parts = trimmedExpression.split(/\s+/);
  if (parts.length !== CRON_FIELD_COUNT) {
    throw new CronParseError(
      `Cron expression must contain ${CRON_FIELD_COUNT} fields`,
    );
  }

  const parsedFields = FIELD_DEFINITIONS.map((definition, index) => {
    const fieldExpression = parts[index];
    if (!fieldExpression) {
      throw new CronParseError(`Missing ${definition.name} field`);
    }
    return parseCronField(fieldExpression, definition);
  });

  return {
    expression: trimmedExpression,
    minute: parsedFields[0] ?? [],
    hour: parsedFields[1] ?? [],
    dayOfMonth: parsedFields[2] ?? [],
    month: parsedFields[3] ?? [],
    dayOfWeek: parsedFields[4] ?? [],
  };
};

export const getNextRun = (
  scheduleOrExpression: CronSchedule | string,
  from: Date = new Date(),
): Date => {
  const schedule = toCronSchedule(scheduleOrExpression);
  const start = roundUpToNextMinute(from);
  const end = new Date(start);
  end.setFullYear(end.getFullYear() + MAX_SEARCH_YEARS);

  for (
    const candidate = new Date(start);
    candidate <= end;
    candidate.setMinutes(candidate.getMinutes() + 1)
  ) {
    if (matchesSchedule(schedule, candidate)) {
      return new Date(candidate);
    }
  }

  throw new CronParseError(
    `No matching run time found within ${MAX_SEARCH_YEARS} years`,
  );
};

export const getNextRuns = (
  scheduleOrExpression: CronSchedule | string,
  count: number,
  from: Date = new Date(),
): Date[] => {
  if (!Number.isInteger(count) || count < 0) {
    throw new CronParseError("Run count must be a non-negative integer");
  }

  const schedule = toCronSchedule(scheduleOrExpression);
  const runs: Date[] = [];
  let cursor = new Date(from);

  while (runs.length < count) {
    const nextRun = getNextRun(schedule, cursor);
    runs.push(nextRun);
    cursor = nextRun;
  }

  return runs;
};

const parseCronField = (
  expression: string,
  definition: CronFieldDefinition,
): number[] => {
  const values = new Set<number>();
  const parts = expression.split(",");

  for (const part of parts) {
    if (!part) {
      throw new CronParseError(`Invalid ${definition.name} field`);
    }

    for (const value of parseCronFieldPart(part, definition)) {
      values.add(definition.normalize?.(value) ?? value);
    }
  }

  if (values.size === 0) {
    throw new CronParseError(`Invalid ${definition.name} field`);
  }

  return [...values].sort((left, right) => left - right);
};

const parseCronFieldPart = (
  expression: string,
  definition: CronFieldDefinition,
): number[] => {
  const [baseExpression, stepExpression, extraStepExpression] =
    expression.split("/");
  if (!baseExpression || extraStepExpression !== undefined) {
    throw new CronParseError(`Invalid ${definition.name} field`);
  }

  const step =
    stepExpression === undefined
      ? 1
      : parsePositiveInteger(stepExpression, `${definition.name} step`);

  if (stepExpression !== undefined && !isStepBase(baseExpression)) {
    throw new CronParseError(
      `${definition.name} steps must use a wildcard or range`,
    );
  }

  const [start, end] = parseFieldBase(baseExpression, definition);
  const values: number[] = [];
  for (let value = start; value <= end; value += step) {
    values.push(value);
  }
  return values;
};

const parseFieldBase = (
  expression: string,
  definition: CronFieldDefinition,
): [number, number] => {
  if (expression === "*") {
    return [definition.min, definition.max];
  }

  if (expression.includes("-")) {
    const [startExpression, endExpression, extraExpression] =
      expression.split("-");
    if (!startExpression || !endExpression || extraExpression !== undefined) {
      throw new CronParseError(`Invalid ${definition.name} range`);
    }

    const start = parseFieldValue(startExpression, definition);
    const end = parseFieldValue(endExpression, definition);
    if (start > end) {
      throw new CronParseError(`${definition.name} range cannot be reversed`);
    }
    return [start, end];
  }

  const value = parseFieldValue(expression, definition);
  return [value, value];
};

const parseFieldValue = (
  expression: string,
  definition: CronFieldDefinition,
): number => {
  const value = parsePositiveInteger(expression, definition.name, true);
  if (value < definition.min || value > definition.max) {
    throw new CronParseError(
      `${definition.name} value must be between ${definition.min} and ${definition.max}`,
    );
  }
  return value;
};

const parsePositiveInteger = (
  expression: string,
  label: string,
  allowZero = false,
): number => {
  if (!/^\d+$/.test(expression)) {
    throw new CronParseError(`${label} must be an integer`);
  }

  const value = Number(expression);
  if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) {
    throw new CronParseError(`${label} must be a positive integer`);
  }

  return value;
};

const isStepBase = (expression: string) => {
  return expression === "*" || expression.includes("-");
};

const toCronSchedule = (scheduleOrExpression: CronSchedule | string) => {
  if (typeof scheduleOrExpression === "string") {
    return parseCronExpression(scheduleOrExpression);
  }
  return scheduleOrExpression;
};

const roundUpToNextMinute = (date: Date): Date => {
  const next = new Date(date);
  next.setSeconds(0, 0);
  next.setMinutes(next.getMinutes() + 1);
  return next;
};

const matchesSchedule = (schedule: CronSchedule, date: Date): boolean => {
  if (!schedule.minute.includes(date.getMinutes())) return false;
  if (!schedule.hour.includes(date.getHours())) return false;
  if (!schedule.month.includes(date.getMonth() + 1)) return false;

  return matchesDay(schedule, date);
};

const matchesDay = (schedule: CronSchedule, date: Date): boolean => {
  const dayOfMonthMatches = schedule.dayOfMonth.includes(date.getDate());
  const dayOfWeekMatches = schedule.dayOfWeek.includes(date.getDay());
  const dayOfMonthRestricted = isRestricted(
    schedule.dayOfMonth,
    FIELD_DEFINITIONS[2],
  );
  const dayOfWeekRestricted = isRestricted(
    schedule.dayOfWeek,
    FIELD_DEFINITIONS[4],
  );

  if (dayOfMonthRestricted && dayOfWeekRestricted) {
    return dayOfMonthMatches || dayOfWeekMatches;
  }

  if (dayOfMonthRestricted) return dayOfMonthMatches;
  if (dayOfWeekRestricted) return dayOfWeekMatches;

  return true;
};

const isRestricted = (
  values: number[],
  definition: CronFieldDefinition | undefined,
) => {
  if (!definition) return true;

  const normalizedValues = new Set(
    Array.from(
      { length: definition.max - definition.min + 1 },
      (_, index) => definition.min + index,
    ).map((value) => definition.normalize?.(value) ?? value),
  );

  return values.length !== normalizedValues.size;
};
