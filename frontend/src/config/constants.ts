/** Shared constants for the IJM frontend. */

export const STATUS_QUEUED = "QUEUED" as const;
export const STATUS_PROFILING = "PROFILING" as const;
export const STATUS_RUNNING = "RUNNING" as const;
export const STATUS_SUCCEEDED = "SUCCEEDED" as const;
export const STATUS_FAILED = "FAILED" as const;
export const STATUS_PREEMPTED = "PREEMPTED" as const;

export const STOPPABLE_STATUSES: ReadonlySet<string> = new Set([STATUS_QUEUED, STATUS_PROFILING, STATUS_RUNNING]);
export const RESUMABLE_STATUSES: ReadonlySet<string> = new Set([STATUS_PREEMPTED, STATUS_FAILED]);

export const POLL_JOBS_MS = 3_000;
export const POLL_JOB_LOGS_MS = 3_000;
export const POLL_NODES_MS = 5_000;
export const POLL_PROFILING_MS = 5_000;

export const NODE_STATUS_IDLE = "idle" as const;
export const NODE_STATUS_BUSY = "busy" as const;

export const JOB_ID_DISPLAY_LENGTH = 8;

/** Format {"A40": 2, "L40S": 1} into a human-readable "2× A40 + 1× L40S" */
export function formatGpuConfig(config: Record<string, number>): string {
  return Object.entries(config)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([type, count]) => `${count}× ${type}`)
    .join(" + ");
}

export const DEFAULT_JOB_PRIORITY = 4;
export const DEFAULT_EPOCHS_TOTAL = 40;
export const DEFAULT_PROFILING_EPOCHS = 3;
export const DEFAULT_IMAGE = "wangrat/ijm-lstm-small:latest";
export const DEFAULT_COMMAND = "python -u train.py";
