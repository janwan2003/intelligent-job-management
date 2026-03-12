import { useState } from "react";
import { Trophy } from "lucide-react";
import { useJobs, useConfigurations, useProfilingResults } from "@/api/hooks";
import { StatusBadge } from "@/components/StatusBadge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { JOB_ID_DISPLAY_LENGTH, formatGpuConfig } from "@/config/constants";
import { cn } from "@/lib/utils";
import type { ProfilingResult, GpuConfiguration } from "@/types/job";

function formatDuration(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = Math.round(seconds % 60);
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`;
}

function configKey(config: Record<string, number>): string {
  return JSON.stringify(config, Object.keys(config).sort());
}

function findResult(
  results: ProfilingResult[] | undefined,
  config: GpuConfiguration,
): ProfilingResult | undefined {
  const key = configKey(config.gpu_config);
  return results?.find((r) => configKey(r.gpu_config) === key);
}

export default function Profiling() {
  const { data: jobs, isLoading: jobsLoading } = useJobs();
  const { data: configurations } = useConfigurations();
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const { data: profilingResults } = useProfilingResults(selectedJobId);

  // Sort jobs: PROFILING first, then RUNNING/QUEUED, then rest
  const statusOrder: Record<string, number> = {
    PROFILING: 0,
    RUNNING: 1,
    QUEUED: 2,
    PREEMPTED: 3,
    FAILED: 4,
    SUCCEEDED: 5,
  };
  const sortedJobs = [...(jobs ?? [])].sort(
    (a, b) => (statusOrder[a.status] ?? 9) - (statusOrder[b.status] ?? 9),
  );

  const totalConfigs = configurations?.length ?? 0;
  const profiledCount = profilingResults?.length ?? 0;
  // Backend returns results sorted by duration ASC, so index 0 is the fastest
  const bestResultId = profilingResults?.[0]?.id;

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-lg font-semibold text-foreground">Profiling</h1>
        <p className="text-sm text-muted-foreground">
          Hardware configuration profiling results per job
        </p>
      </div>

      {jobsLoading ? (
        <Skeleton className="h-64 w-full" />
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* Left: Job list */}
          <div className="space-y-1.5">
            <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider mb-2">
              Jobs
            </h2>
            <div className="space-y-1 max-h-[calc(100vh-12rem)] overflow-y-auto pr-1">
              {sortedJobs.map((job) => (
                <button
                  key={job.id}
                  type="button"
                  onClick={() => setSelectedJobId(job.id)}
                  className={cn(
                    "w-full text-left rounded-md border px-3 py-2 transition-colors",
                    selectedJobId === job.id
                      ? "border-primary bg-primary/5"
                      : "border-border bg-card hover:bg-muted/50",
                  )}
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="font-mono text-xs font-medium">
                      {job.id.slice(0, JOB_ID_DISPLAY_LENGTH)}
                    </span>
                    <StatusBadge status={job.status} />
                  </div>
                  <p className="font-mono text-[11px] text-muted-foreground mt-0.5 truncate">
                    {job.image}
                  </p>
                </button>
              ))}
              {sortedJobs.length === 0 && (
                <p className="text-sm text-muted-foreground py-4 text-center">
                  No jobs submitted yet.
                </p>
              )}
            </div>
          </div>

          {/* Right: Configuration grid */}
          <div className="lg:col-span-2">
            {selectedJobId ? (
              <div className="space-y-3">
                {/* Progress header */}
                <div className="flex items-center justify-between">
                  <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                    Configuration Results
                  </h2>
                  <span className="text-xs font-mono text-muted-foreground">
                    {profiledCount} / {totalConfigs} profiled
                  </span>
                </div>

                {/* Progress bar */}
                {totalConfigs > 0 && (
                  <div className="h-1.5 w-full rounded-full bg-muted overflow-hidden">
                    <div
                      className="h-full rounded-full bg-primary transition-all duration-500"
                      style={{
                        width: `${(profiledCount / totalConfigs) * 100}%`,
                      }}
                    />
                  </div>
                )}

                {/* Config table */}
                <div className="rounded-lg border border-border bg-card">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="text-xs">Configuration</TableHead>
                        <TableHead className="text-xs">Duration</TableHead>
                        <TableHead className="text-xs">Node</TableHead>
                        <TableHead className="text-xs w-20">Status</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {configurations?.map((config) => {
                        const result = findResult(profilingResults, config);
                        const isBest = result != null && result.id === bestResultId;
                        return (
                          <TableRow
                            key={configKey(config.gpu_config)}
                            className={cn(isBest && "bg-emerald-500/5")}
                          >
                            <TableCell className="font-mono text-xs py-2.5">
                              {isBest && (
                                <Trophy className="inline h-3.5 w-3.5 text-emerald-500 mr-1.5 -mt-0.5" />
                              )}
                              {formatGpuConfig(config.gpu_config)}
                            </TableCell>
                            <TableCell className="font-mono text-xs py-2.5">
                              {result ? (
                                formatDuration(result.duration_seconds)
                              ) : (
                                <span className="text-muted-foreground">—</span>
                              )}
                            </TableCell>
                            <TableCell className="font-mono text-xs py-2.5 text-muted-foreground">
                              {result?.node_id ?? "—"}
                            </TableCell>
                            <TableCell className="py-2.5">
                              {result ? (
                                <span className="inline-flex items-center rounded-full bg-emerald-500/10 px-2 py-0.5 text-[10px] font-medium text-emerald-600">
                                  Done
                                </span>
                              ) : (
                                <span className="inline-flex items-center rounded-full bg-muted px-2 py-0.5 text-[10px] font-medium text-muted-foreground">
                                  Pending
                                </span>
                              )}
                            </TableCell>
                          </TableRow>
                        );
                      })}
                      {(!configurations || configurations.length === 0) && (
                        <TableRow>
                          <TableCell
                            colSpan={4}
                            className="text-center text-sm text-muted-foreground py-8"
                          >
                            No hardware configurations available.
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </div>
              </div>
            ) : (
              <div className="flex items-center justify-center h-64 rounded-lg border border-dashed border-border">
                <p className="text-sm text-muted-foreground">
                  Select a job to view profiling results
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
