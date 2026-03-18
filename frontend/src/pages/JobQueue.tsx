import { useState, useRef, useEffect } from "react";
import type { ColumnDef } from "@tanstack/react-table";
import { StatusBadge } from "@/components/StatusBadge";
import { DataTable } from "@/components/DataTable";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useJobs, useStopJob, useResumeJob, useDeleteJob, useClearAllJobs, useJobLogs } from "@/api/hooks";
import { FEATURE_JOB_EXTENDED_FIELDS, FEATURE_PROFILING_SCHEDULER } from "@/config/features";
import { STOPPABLE_STATUSES, RESUMABLE_STATUSES, JOB_ID_DISPLAY_LENGTH, formatGpuConfig } from "@/config/constants";
import type { ApiJob } from "@/types/job";
import { format } from "date-fns";
import { Square, Play, Trash2, Trash } from "lucide-react";
import { toast } from "sonner";

export default function JobQueue() {
  const { data: jobs, isLoading } = useJobs();
  const stopJob = useStopJob();
  const resumeJob = useResumeJob();
  const deleteJob = useDeleteJob();
  const clearAllJobs = useClearAllJobs();
  const [logJobId, setLogJobId] = useState<string | null>(null);
  const { data: logs } = useJobLogs(logJobId);
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs]);

  const handleStop = (jobId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    stopJob.mutate(jobId, {
      onSuccess: () => toast.success(`Job ${jobId.slice(0, JOB_ID_DISPLAY_LENGTH)} stopped`),
      onError: (err) => toast.error(err.message || "Failed to stop job"),
    });
  };

  const handleResume = (jobId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    resumeJob.mutate(jobId, {
      onSuccess: () => toast.success(`Job ${jobId.slice(0, JOB_ID_DISPLAY_LENGTH)} resumed`),
      onError: (err) => toast.error(err.message || "Failed to resume job"),
    });
  };

  const handleDelete = (jobId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    deleteJob.mutate(jobId, {
      onSuccess: () => toast.success(`Job ${jobId.slice(0, JOB_ID_DISPLAY_LENGTH)} deleted`),
      onError: (err) => toast.error(err.message || "Failed to delete job"),
    });
  };

  const baseColumns: ColumnDef<ApiJob, unknown>[] = [
    {
      accessorKey: "id",
      header: "ID",
      cell: ({ row }) => (
        <span className="font-mono font-medium text-xs">{row.original.id.slice(0, JOB_ID_DISPLAY_LENGTH)}</span>
      ),
      enableSorting: false,
    },
    {
      accessorKey: "image",
      header: "Image",
      cell: ({ getValue }) => (
        <span className="font-mono text-xs">{getValue() as string}</span>
      ),
    },
    {
      accessorKey: "command",
      header: "Command",
      cell: ({ row }) => (
        <span className="font-mono text-xs max-w-48 truncate block">{row.original.command.join(" ")}</span>
      ),
      enableSorting: false,
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => <StatusBadge status={row.original.status} />,
      filterFn: (row, _columnId, filterValue: string) => {
        return row.original.status.toLowerCase().includes(filterValue.toLowerCase());
      },
    },
    {
      accessorKey: "progress",
      header: "Progress",
      cell: ({ getValue }) => (
        <span className="font-mono text-xs text-muted-foreground">{(getValue() as string) ?? "—"}</span>
      ),
    },
  ];

  const extendedColumns: ColumnDef<ApiJob, unknown>[] = FEATURE_JOB_EXTENDED_FIELDS
    ? [
        {
          accessorKey: "priority",
          header: "Priority",
          cell: ({ getValue }) => {
            const v = getValue() as number;
            return <span className="font-mono text-xs text-muted-foreground">{v}</span>;
          },
        },
        {
          accessorKey: "epochs_total",
          header: "Epochs",
          cell: ({ getValue }) => {
            const v = getValue() as number | null | undefined;
            return (
              <span className="font-mono text-xs text-muted-foreground">
                {v !== null && v !== undefined ? v : "—"}
              </span>
            );
          },
        },
        {
          accessorKey: "deadline",
          header: "Deadline",
          cell: ({ getValue }) => {
            const v = getValue() as string | null | undefined;
            return (
              <span className="font-mono text-xs text-muted-foreground">
                {v ? format(new Date(v), "MM/dd HH:mm") : "—"}
              </span>
            );
          },
        },
        {
          accessorKey: "assigned_node",
          header: "Node",
          cell: ({ getValue }) => {
            const v = getValue() as string | null | undefined;
            return <span className="font-mono text-xs text-muted-foreground">{v ?? "—"}</span>;
          },
        },
      ]
    : [];

  const profilingColumns: ColumnDef<ApiJob, unknown>[] = FEATURE_PROFILING_SCHEDULER
    ? [
        {
          id: "gpu_config",
          header: "GPU Config",
          cell: ({ row }) => {
            const job = row.original;
            if (!job.assigned_gpu_config) return <span className="font-mono text-xs text-muted-foreground">—</span>;
            return (
              <span className="font-mono text-xs">
                {formatGpuConfig(job.assigned_gpu_config)}
              </span>
            );
          },
        },
        {
          accessorKey: "estimated_duration",
          header: "ETA",
          cell: ({ getValue }) => {
            const v = getValue() as number | null | undefined;
            if (v === null || v === undefined) return <span className="font-mono text-xs text-muted-foreground">—</span>;
            const mins = Math.floor(v / 60);
            const secs = (v % 60).toFixed(2);
            return <span className="font-mono text-xs text-muted-foreground">{mins > 0 ? `${mins}m ` : ""}{secs}s</span>;
          },
        },
      ]
    : [];

  const tailColumns: ColumnDef<ApiJob, unknown>[] = [
    {
      accessorKey: "created_at",
      header: "Created",
      cell: ({ getValue }) => (
        <span className="font-mono text-xs text-muted-foreground">
          {format(new Date(getValue() as string), "MM/dd HH:mm")}
        </span>
      ),
    },
    {
      accessorKey: "exit_code",
      header: "Exit",
      cell: ({ getValue }) => {
        const v = getValue() as number | null | undefined;
        return (
          <span className="font-mono text-xs text-muted-foreground">
            {v !== null && v !== undefined ? v : "—"}
          </span>
        );
      },
    },
    {
      id: "actions",
      header: "",
      enableSorting: false,
      cell: ({ row }) => {
        const job = row.original;
        return (
          <div className="flex gap-1" onClick={(e) => e.stopPropagation()}>
            {STOPPABLE_STATUSES.has(job.status) && (
              <Button
                variant="ghost"
                size="sm"
                className="h-7 px-2 text-destructive hover:text-destructive hover:bg-destructive/10"
                onClick={(e) => handleStop(job.id, e)}
              >
                <Square className="h-3 w-3 mr-1" />
                Stop
              </Button>
            )}
            {RESUMABLE_STATUSES.has(job.status) && (
              <Button
                variant="ghost"
                size="sm"
                className="h-7 px-2"
                onClick={(e) => handleResume(job.id, e)}
              >
                <Play className="h-3 w-3 mr-1" />
                Resume
              </Button>
            )}
            <Button
              variant="ghost"
              size="sm"
              className="h-7 px-2 text-muted-foreground hover:text-destructive hover:bg-destructive/10"
              onClick={(e) => handleDelete(job.id, e)}
            >
              <Trash2 className="h-3 w-3" />
            </Button>
          </div>
        );
      },
    },
  ];

  const columns: ColumnDef<ApiJob, unknown>[] = [...baseColumns, ...extendedColumns, ...profilingColumns, ...tailColumns];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Job Queue</h1>
          <p className="text-sm text-muted-foreground">
            {isLoading ? "Loading..." : `${jobs?.length ?? 0} jobs in scheduler`}
          </p>
        </div>
        {jobs && jobs.length > 0 && (
          <Button
            variant="outline"
            size="sm"
            className="text-destructive hover:text-destructive hover:bg-destructive/10"
            disabled={clearAllJobs.isPending}
            onClick={() => {
              clearAllJobs.mutate(undefined, {
                onSuccess: () => toast.success("All jobs cleared"),
                onError: (err) => toast.error(err.message || "Failed to clear jobs"),
              });
            }}
          >
            <Trash className="h-3.5 w-3.5 mr-1.5" />
            {clearAllJobs.isPending ? "Clearing..." : "Clear All"}
          </Button>
        )}
      </div>

      {isLoading ? (
        <Skeleton className="h-32 w-full" />
      ) : (
        <DataTable
          columns={columns}
          data={jobs ?? []}
          filterColumn="status"
          filterPlaceholder="Filter by status..."
          onRowClick={(job) => setLogJobId(job.id)}
        />
      )}

      {/* Log Viewer Dialog */}
      <Dialog open={logJobId !== null} onOpenChange={(open) => { if (!open) setLogJobId(null); }}>
        <DialogContent className="max-w-3xl max-h-[80vh] flex flex-col">
          <DialogHeader>
            <DialogTitle className="font-mono text-sm">
              Logs — {logJobId?.slice(0, JOB_ID_DISPLAY_LENGTH)}
            </DialogTitle>
          </DialogHeader>
          <div className="flex-1 overflow-auto bg-background rounded border border-border p-3 min-h-[300px]">
            <pre className="font-mono text-xs whitespace-pre-wrap text-foreground">
              {logs || "No logs available yet."}
            </pre>
            <div ref={logEndRef} />
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
