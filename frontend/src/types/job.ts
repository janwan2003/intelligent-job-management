/** Job status values from the API */
export type ApiJobStatus = "QUEUED" | "PROFILING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "PREEMPTED";

/** Job shape as returned by the API */
export interface ApiJob {
  id: string;
  image: string;
  command: string[];
  status: ApiJobStatus;
  created_at: string;
  updated_at: string;
  container_name?: string;
  exit_code?: number;
  progress?: string;
  priority: number;
  deadline?: string;
  batch_size?: number;
  epochs_total: number;
  profiling_epochs_no: number;
  assigned_node?: string;
  required_memory_gb?: number;
  assigned_gpu_config?: Record<string, number>;
  estimated_duration?: number;
  is_profiling_run?: boolean;
  log_interval?: number;
}

/** Payload for creating a new job */
export interface CreateJobPayload {
  image: string;
  command: string[];
  Priority?: number;
  deadline?: string;
  profilingEpochsNo: number;
  epochsTotal: number;
  logInterval?: number;
}

/** Single profiling result as returned by GET /profiling-results/{job_id} */
export interface ProfilingResult {
  id: string;
  gpu_config: Record<string, number>;
  node_id: string;
  duration_seconds: number;
  created_at: string;
}

/** Hardware configuration as returned by GET /configurations */
export interface GpuConfiguration {
  gpu_config: Record<string, number>;
}

/** GPU hardware resources attached to a node */
export interface NodeResources {
  gpu_type: string;
  gpu_count: number;
  memory_per_gpu_gb: number;
}

/** Node shape as returned by the API */
export interface ApiNode {
  id: string;
  is_for_profiling: boolean;
  cost: number;
  resources: NodeResources[];
  status: "idle" | "busy";
  current_job_ids: string[];
}
