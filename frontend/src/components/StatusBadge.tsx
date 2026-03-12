import { cva } from "class-variance-authority";
import { cn } from "@/lib/utils";
import type { ApiJobStatus } from "@/types/job";
import {
  STATUS_QUEUED,
  STATUS_PROFILING,
  STATUS_RUNNING,
  STATUS_SUCCEEDED,
  STATUS_FAILED,
  STATUS_PREEMPTED,
} from "@/config/constants";

const statusVariants = cva("inline-flex items-center rounded px-2 py-0.5 text-xs font-medium font-mono", {
  variants: {
    status: {
      [STATUS_RUNNING]: "bg-status-running/15 text-status-running",
      [STATUS_PROFILING]: "bg-status-profiling/15 text-status-profiling",
      [STATUS_QUEUED]: "bg-status-queued/15 text-status-queued",
      [STATUS_SUCCEEDED]: "bg-status-completed/15 text-status-completed",
      [STATUS_FAILED]: "bg-status-late/15 text-status-late",
      [STATUS_PREEMPTED]: "bg-status-preempted/15 text-status-preempted",
    },
  },
});

export function StatusBadge({ status, className }: { status: ApiJobStatus; className?: string }) {
  return (
    <span className={cn(statusVariants({ status }), className)}>
      {(status === STATUS_RUNNING || status === STATUS_PROFILING) && <span className="mr-1.5 h-1.5 w-1.5 rounded-full bg-current animate-pulse-dot" />}
      {status}
    </span>
  );
}
