import type { ApiJob, ApiNode, CreateJobPayload } from "@/types/job";

const API_BASE = import.meta.env.VITE_API_URL ?? "http://localhost:8000";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({})) as { detail?: string };
    throw new Error(body.detail ?? `Request failed: ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  getJobs: () => apiFetch<ApiJob[]>("/jobs"),

  createJob: (payload: CreateJobPayload) =>
    apiFetch<ApiJob>("/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),

  stopJob: (id: string) =>
    apiFetch<Record<string, never>>(`/jobs/${id}/stop`, { method: "POST" }),

  resumeJob: (id: string) =>
    apiFetch<Record<string, never>>(`/jobs/${id}/resume`, { method: "POST" }),

  deleteJob: (id: string) =>
    apiFetch<Record<string, never>>(`/jobs/${id}`, { method: "DELETE" }),

  getJobLogs: async (id: string): Promise<string> => {
    const res = await fetch(`${API_BASE}/jobs/${id}/logs`);
    if (!res.ok) {
      throw new Error(`Failed to fetch logs: ${res.status}`);
    }
    return res.text();
  },

  uploadImage: async (file: File): Promise<{ image: string; message: string }> => {
    const formData = new FormData();
    formData.append("file", file);
    const res = await fetch(`${API_BASE}/images/upload`, {
      method: "POST",
      body: formData,
    });
    if (!res.ok) {
      const body = await res.json().catch(() => ({})) as { detail?: string };
      throw new Error(body.detail ?? "Upload failed");
    }
    return res.json() as Promise<{ image: string; message: string }>;
  },

  getNodes: () => apiFetch<ApiNode[]>("/nodes"),
};
