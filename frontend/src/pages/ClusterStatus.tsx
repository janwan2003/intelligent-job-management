import { Cpu, FlaskConical } from "lucide-react";
import { useNodes } from "@/api/hooks";
import { Skeleton } from "@/components/ui/skeleton";
import { NODE_STATUS_BUSY, JOB_ID_DISPLAY_LENGTH } from "@/config/constants";

export default function ClusterStatus() {
  const { data: nodes, isLoading } = useNodes();

  const totalNodes = nodes?.length ?? 0;
  const busyNodes = nodes?.filter((n) => n.status === NODE_STATUS_BUSY).length ?? 0;

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-lg font-semibold text-foreground">Cluster Status</h1>
        <p className="text-sm text-muted-foreground">
          {isLoading ? "Loading..." : `${totalNodes} nodes registered — ${busyNodes} busy`}
        </p>
      </div>

      {isLoading ? (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          <Skeleton className="h-32" />
          <Skeleton className="h-32" />
          <Skeleton className="h-32" />
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {nodes?.map((node) => (
            <div key={node.id} className="rounded-lg border border-border bg-card p-4 space-y-3">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  {node.is_for_profiling ? (
                    <FlaskConical className="h-4 w-4 text-muted-foreground" />
                  ) : (
                    <Cpu className="h-4 w-4 text-muted-foreground" />
                  )}
                  <span className="text-sm font-semibold font-mono text-card-foreground">{node.id}</span>
                </div>
                <span
                  className={`inline-flex items-center rounded px-2 py-0.5 text-xs font-medium font-mono ${
                    node.status === NODE_STATUS_BUSY
                      ? "bg-status-running/15 text-status-running"
                      : "bg-status-queued/15 text-status-queued"
                  }`}
                >
                  {node.status === NODE_STATUS_BUSY ? "Busy" : "Idle"}
                </span>
              </div>

              <div className="space-y-1.5 text-xs">
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Role</span>
                  <span className="text-card-foreground">
                    {node.is_for_profiling ? "Profiling" : "Compute"}
                  </span>
                </div>
                {node.resources.length > 0 && (
                  <>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">GPU</span>
                      <span className="text-card-foreground font-mono">
                        {node.resources.map((r, i) => (
                          <span key={i}>
                            {i > 0 && " + "}
                            {r.gpu_count}&times; {r.gpu_type}
                          </span>
                        ))}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">VRAM</span>
                      <span className="text-card-foreground font-mono">
                        {node.resources.reduce((sum, r) => sum + r.gpu_count * r.memory_per_gpu_gb, 0)} GB
                      </span>
                    </div>
                  </>
                )}
                <div className="flex justify-between">
                  <span className="text-muted-foreground">Cost</span>
                  <span className="text-card-foreground font-mono">{node.cost.toFixed(4)} &euro;/h</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-muted-foreground">{node.current_job_ids.length === 1 ? "Job" : "Jobs"}</span>
                  <span className="text-card-foreground font-mono">
                    {node.current_job_ids.length > 0
                      ? node.current_job_ids.map((id) => id.slice(0, JOB_ID_DISPLAY_LENGTH)).join(", ")
                      : "—"}
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
