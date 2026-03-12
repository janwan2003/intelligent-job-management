import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "./client";
import type { CreateJobPayload } from "@/types/job";

const JOBS_KEY = ["jobs"] as const;
const NODES_KEY = ["nodes"] as const;

/** Poll jobs every 3 seconds */
export function useJobs() {
  return useQuery({
    queryKey: JOBS_KEY,
    queryFn: api.getJobs,
    refetchInterval: 3000,
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

export function useUploadImage() {
  return useMutation({
    mutationFn: (file: File) => api.uploadImage(file),
  });
}

/** Fetch job logs, polling every 3s while enabled */
export function useJobLogs(jobId: string | null) {
  return useQuery({
    queryKey: ["jobLogs", jobId],
    queryFn: () => api.getJobLogs(jobId!),
    enabled: !!jobId,
    refetchInterval: 3000,
  });
}

/** Poll cluster nodes every 5 seconds */
export function useNodes() {
  return useQuery({
    queryKey: NODES_KEY,
    queryFn: api.getNodes,
    refetchInterval: 5000,
  });
}
