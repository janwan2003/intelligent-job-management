import { Briefcase, Server } from "lucide-react";
import { MetricCard } from "@/components/MetricCard";
import { useJobs, useNodes } from "@/api/hooks";
import { format } from "date-fns";
import {
  STATUS_QUEUED,
  STATUS_PROFILING,
  STATUS_RUNNING,
  STATUS_SUCCEEDED,
  STATUS_FAILED,
  NODE_STATUS_BUSY,
} from "@/config/constants";
import { Skeleton } from "@/components/ui/skeleton";

export default function Dashboard() {
  const { data: jobs, isLoading } = useJobs();
  const { data: nodes } = useNodes();

  const total = jobs?.length ?? 0;
  const queued = jobs?.filter((j) => j.status === STATUS_QUEUED).length ?? 0;
  const profiling = jobs?.filter((j) => j.status === STATUS_PROFILING).length ?? 0;
  const running = jobs?.filter((j) => j.status === STATUS_RUNNING).length ?? 0;
  const completed = jobs?.filter((j) => j.status === STATUS_SUCCEEDED || j.status === STATUS_FAILED).length ?? 0;

  const totalNodes = nodes?.length ?? 0;
  const busyNodes = nodes?.filter((n) => n.status === NODE_STATUS_BUSY).length ?? 0;

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-lg font-semibold text-foreground">Dashboard</h1>
        <p className="text-sm text-muted-foreground">Cluster overview — {format(new Date(), "MMM d, yyyy HH:mm")}</p>
      </div>

      {/* Metrics */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {isLoading ? (
          <>
            <Skeleton className="h-24" />
            <Skeleton className="h-24" />
          </>
        ) : (
          <MetricCard
            title="Total Jobs"
            value={String(total)}
            subtitle={`${queued} queued · ${profiling} profiling · ${running} running · ${completed} done`}
            icon={Briefcase}
            accentColor="bg-metric-jobs/10 text-metric-jobs"
          />
        )}

        <MetricCard
          title="Cluster Nodes"
          value={String(totalNodes)}
          subtitle={`${busyNodes} busy · ${totalNodes - busyNodes} idle`}
          icon={Server}
          accentColor="bg-metric-nodes/10 text-metric-nodes"
        />
      </div>
    </div>
  );
}
