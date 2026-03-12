import { cva } from "class-variance-authority";
import { cn } from "@/lib/utils";
import type { ApiJobStatus } from "@/types/job";

const statusVariants = cva("inline-flex items-center rounded px-2 py-0.5 text-xs font-medium font-mono", {
  variants: {
    status: {
      RUNNING: "bg-status-running/15 text-status-running",
      QUEUED: "bg-status-queued/15 text-status-queued",
      SUCCEEDED: "bg-status-completed/15 text-status-completed",
      FAILED: "bg-status-late/15 text-status-late",
      PREEMPTED: "bg-status-preempted/15 text-status-preempted",
    },
  },
});

export function StatusBadge({ status, className }: { status: ApiJobStatus; className?: string }) {
  return (
    <span className={cn(statusVariants({ status }), className)}>
      {status === "RUNNING" && <span className="mr-1.5 h-1.5 w-1.5 rounded-full bg-current animate-pulse-dot" />}
      {status}
    </span>
  );
}
