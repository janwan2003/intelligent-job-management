import { Briefcase, Server, Zap, DollarSign } from "lucide-react";
import { MetricCard } from "@/components/MetricCard";
import { useJobs, useNodes } from "@/api/hooks";
import { format } from "date-fns";
import {
  FEATURE_DASHBOARD_ACTIVE_NODES,
  FEATURE_DASHBOARD_POWER_DRAW,
  FEATURE_DASHBOARD_SESSION_COST,
  FEATURE_DASHBOARD_ACTIVITY_LOG,
} from "@/config/features";
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

        {FEATURE_DASHBOARD_ACTIVE_NODES && (
          <MetricCard
            title="Cluster Nodes"
            value={String(totalNodes)}
            subtitle={`${busyNodes} busy · ${totalNodes - busyNodes} idle`}
            icon={Server}
            accentColor="bg-metric-nodes/10 text-metric-nodes"
          />
        )}
        {FEATURE_DASHBOARD_POWER_DRAW && (
          <MetricCard
            title="Power Draw"
            value="—"
            subtitle="Est. cluster consumption"
            icon={Zap}
            accentColor="bg-metric-power/10 text-metric-power"
          />
        )}
        {FEATURE_DASHBOARD_SESSION_COST && (
          <MetricCard
            title="Session Cost"
            value="—"
            subtitle="Current session"
            icon={DollarSign}
            accentColor="bg-metric-cost/10 text-metric-cost"
          />
        )}
      </div>

      {/* Activity Log */}
      {FEATURE_DASHBOARD_ACTIVITY_LOG && (
        <div className="rounded-lg border border-border bg-card">
          <div className="px-4 py-3 border-b border-border">
            <h2 className="text-sm font-medium text-card-foreground">Recent Activity</h2>
          </div>
          <div className="px-4 py-6 text-sm text-muted-foreground text-center">No activity data available yet.</div>
        </div>
      )}
    </div>
  );
}
