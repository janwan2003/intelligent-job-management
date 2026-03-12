/** Job status values from the API */
export type ApiJobStatus = "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "PREEMPTED";

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
  // ANDREAS extended fields
  priority: number;
  deadline?: string;
  batch_size?: number;
  epochs_total?: number;
  profiling_epochs_no?: number;
  script_path?: string;
  directory_to_mount?: string;
  assigned_node?: string;
}

/** Payload for creating a new job (legacy format) */
export interface CreateJobPayload {
  image?: string;
  command?: string[];
  dockerImage?: string;
  scriptPath?: string;
  directoryToMount?: string;
  Priority?: number;
  deadline?: string;
  batchSize?: number;
  profilingEpochsNo?: number;
  epochsTotal?: number;
}

/** Node shape as returned by the API */
export interface ApiNode {
  id: string;
  is_for_profiling: boolean;
  cost: number;
  status: "idle" | "busy";
  current_job_id?: string;
}
