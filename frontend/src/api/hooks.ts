import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import type { CreateJobPayload } from "@/types/job";
import { POLL_JOBS_MS, POLL_JOB_LOGS_MS, POLL_NODES_MS, POLL_PROFILING_MS } from "@/config/constants";

const JOBS_KEY = ["jobs"] as const;
const NODES_KEY = ["nodes"] as const;

/** Poll jobs every 3 seconds */
export function useJobs() {
  return useQuery({
    queryKey: JOBS_KEY,
    queryFn: api.getJobs,
    refetchInterval: POLL_JOBS_MS,
  });
}

export function useCreateJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (payload: CreateJobPayload) => api.createJob(payload),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: JOBS_KEY }); },
  });
}

export function useStopJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.stopJob(id),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: JOBS_KEY }); },
  });
}

export function useResumeJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.resumeJob(id),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: JOBS_KEY }); },
  });
}

export function useDeleteJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.deleteJob(id),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: JOBS_KEY }); },
  });
}

export function useClearAllJobs() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => api.clearAllJobs(),
    onSuccess: () => { void qc.invalidateQueries({ queryKey: JOBS_KEY }); },
  });
}


/** Fetch job logs, polling every 3s while enabled */
export function useJobLogs(jobId: string | null) {
  return useQuery({
    queryKey: ["jobLogs", jobId],
    queryFn: () => api.getJobLogs(jobId!),
    enabled: !!jobId,
    refetchInterval: POLL_JOB_LOGS_MS,
  });
}

/** Poll cluster nodes every 5 seconds */
export function useNodes() {
  return useQuery({
    queryKey: NODES_KEY,
    queryFn: api.getNodes,
    refetchInterval: POLL_NODES_MS,
  });
}

/** Fetch profiling results for a specific job, polling every 5s */
export function useProfilingResults(jobId: string | null) {
  return useQuery({
    queryKey: ["profilingResults", jobId],
    queryFn: () => api.getProfilingResults(jobId!),
    enabled: !!jobId,
    refetchInterval: POLL_PROFILING_MS,
  });
}

/** Fetch all valid hardware configurations (rarely changes) */
export function useConfigurations() {
  return useQuery({
    queryKey: ["configurations"],
    queryFn: api.getConfigurations,
    staleTime: 60_000,
  });
}
