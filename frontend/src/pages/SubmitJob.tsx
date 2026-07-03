import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useCreateJob } from "@/api/hooks";
import {
  DEFAULT_IMAGE,
  DEFAULT_JOB_PRIORITY,
  DEFAULT_EPOCHS_TOTAL,
  DEFAULT_PROFILING_EPOCHS,
} from "@/config/constants";
import { toast } from "sonner";
import { HelpCircle } from "lucide-react";


function FieldHint({ text }: { text: string }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <HelpCircle className="inline h-3.5 w-3.5 text-muted-foreground cursor-help ml-1" />
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs text-xs">
        {text}
      </TooltipContent>
    </Tooltip>
  );
}

export default function SubmitJob() {
  const navigate = useNavigate();
  const createJob = useCreateJob();

  const [jobId, setJobId] = useState("");
  const [image, setImage] = useState(DEFAULT_IMAGE);
  const [scriptPath, setScriptPath] = useState("");
  const [directoryToMount, setDirectoryToMount] = useState("");
  const [priority, setPriority] = useState(DEFAULT_JOB_PRIORITY);
  const [deadlineHours, setDeadlineHours] = useState<number | undefined>(undefined);
  const [deadlineMinutes, setDeadlineMinutes] = useState<number | undefined>(undefined);
  const [profilingEpochsNo, setProfilingEpochsNo] = useState(DEFAULT_PROFILING_EPOCHS);
  const [epochsTotal, setEpochsTotal] = useState(DEFAULT_EPOCHS_TOTAL);
  const [batchSize, setBatchSize] = useState<number | undefined>(undefined);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (!jobId.trim()) {
      toast.error("Job ID is required");
      return;
    }
    if (!image.trim()) {
      toast.error("Docker image is required");
      return;
    }

    createJob.mutate(
      {
        job_id: jobId.trim(),
        dockerImage: image.trim(),
        epochsTotal: epochsTotal,
        profilingEpochsNo: profilingEpochsNo,
        Priority: priority,
        ...(batchSize !== undefined && { batchSize }),
        ...(scriptPath.trim() && { scriptPath: scriptPath.trim() }),
        ...(directoryToMount.trim() && { directoryToMount: directoryToMount.trim() }),
        // Deadline entered as a relative offset (hours + minutes from now)
        // and converted to an absolute UTC instant for the backend.
        ...(((deadlineHours ?? 0) !== 0 || (deadlineMinutes ?? 0) !== 0) && {
          deadline: new Date(
            Date.now() + ((deadlineHours ?? 0) * 60 + (deadlineMinutes ?? 0)) * 60_000,
          ).toISOString(),
        }),
      },
      {
        onSuccess: () => {
          toast.success("Job submitted successfully");
          navigate("/jobs");
        },
        onError: (err) => {
          toast.error(err.message || "Failed to submit job");
        },
      },
    );
  };

  return (
    <TooltipProvider delayDuration={200}>
      <div className="space-y-6 max-w-2xl">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Submit Job</h1>
          <p className="text-sm text-muted-foreground">Queue a new training task</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          {/* Required: Container & Execution */}
          <div className="rounded-lg border border-border bg-card p-5 space-y-4">
            <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Container</h2>

            <div className="grid grid-cols-2 gap-4">
              {/* Job ID */}
              <div className="space-y-1.5">
                <Label htmlFor="jobId" className="text-xs">
                  Job ID <span className="text-destructive">*</span>
                  <FieldHint text="Type identifier for this job (e.g. 'LSTM-small'). Reusing the same ID across submissions correlates profiling results." />
                </Label>
                <Input
                  id="jobId"
                  value={jobId}
                  onChange={(e) => setJobId(e.target.value)}
                  placeholder="e.g. LSTM-small"
                  className="font-mono text-sm"
                  required
                />
              </div>

              {/* Docker Image */}
              <div className="space-y-1.5">
                <Label htmlFor="image" className="text-xs">
                  Docker Image <span className="text-destructive">*</span>
                  <FieldHint text="The Docker image to run. Must be pre-built and available on the host (e.g. via docker build)." />
                </Label>
                <Input
                  id="image"
                  value={image}
                  onChange={(e) => setImage(e.target.value)}
                  placeholder="e.g. ijm-lstm-small:dev"
                  className="font-mono text-sm"
                  required
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              {/* Script Path */}
              <div className="space-y-1.5">
                <Label htmlFor="scriptPath" className="text-xs">
                  Script Path
                  <FieldHint text="Path to the training script inside the container or mounted directory (e.g. 'train.py'). If set, overrides the Docker CMD." />
                </Label>
                <Input
                  id="scriptPath"
                  value={scriptPath}
                  onChange={(e) => setScriptPath(e.target.value)}
                  placeholder="optional"
                  className="font-mono text-sm"
                />
              </div>

              {/* Directory to Mount */}
              <div className="space-y-1.5">
                <Label htmlFor="directoryToMount" className="text-xs">
                  Directory to Mount
                  <FieldHint text="Host directory to mount into the container at /workspace (e.g. '/data/my-project'). Used for custom training scripts and data." />
                </Label>
                <Input
                  id="directoryToMount"
                  value={directoryToMount}
                  onChange={(e) => setDirectoryToMount(e.target.value)}
                  placeholder="optional"
                  className="font-mono text-sm"
                />
              </div>
            </div>
          </div>

          {/* Training Parameters */}
            <div className="rounded-lg border border-border bg-card p-5 space-y-4">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Training Parameters</h2>

              <div className="grid grid-cols-2 gap-4">
                {/* Epochs Total */}
                <div className="space-y-1.5">
                  <Label htmlFor="epochsTotal" className="text-xs">
                    Total Epochs
                    <FieldHint text="Passed as EPOCHS_TOTAL env var. Your training script should read os.environ.get('EPOCHS_TOTAL') to know when to stop." />
                  </Label>
                  <Input
                    id="epochsTotal"
                    type="number"
                    min={1}
                    value={epochsTotal}
                    onChange={(e) => setEpochsTotal(Number(e.target.value))}
                    className="font-mono text-sm"
                  />
                </div>

                {/* Profiling Epochs */}
                <div className="space-y-1.5">
                  <Label htmlFor="profilingEpochsNo" className="text-xs">
                    Profiling Epochs
                    <FieldHint text="Number of epochs for each profiling run. The first epoch is treated as warmup (GPU caches, JIT) and excluded from timing. Minimum 3 for accurate results." />
                  </Label>
                  <Input
                    id="profilingEpochsNo"
                    type="number"
                    min={3}
                    value={profilingEpochsNo}
                    onChange={(e) => setProfilingEpochsNo(Number(e.target.value))}
                    className="font-mono text-sm"
                  />
                </div>

                {/* Batch Size */}
                <div className="space-y-1.5">
                  <Label htmlFor="batchSize" className="text-xs">
                    Batch Size
                    <FieldHint text="Passed as BATCH_SIZE env var. Your training script should read os.environ.get('BATCH_SIZE') for the batch size." />
                  </Label>
                  <Input
                    id="batchSize"
                    type="number"
                    min={1}
                    value={batchSize ?? ""}
                    onChange={(e) => setBatchSize(e.target.value ? Number(e.target.value) : undefined)}
                    placeholder="optional"
                    className="font-mono text-sm"
                  />
                </div>

              </div>
            </div>

          {/* Scheduling */}
            <div className="rounded-lg border border-border bg-card p-5 space-y-4">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Scheduling</h2>

              <div className="grid grid-cols-2 gap-4">
                {/* Priority */}
                <div className="space-y-1.5">
                  <Label htmlFor="priority" className="text-xs">
                    Priority (1-5, 5 = highest)
                    <FieldHint text="Higher-priority jobs are scheduled first when resources are contested. Does not affect the container." />
                  </Label>
                  <Input
                    id="priority"
                    type="number"
                    min={1}
                    max={5}
                    value={priority}
                    onChange={(e) => setPriority(Number(e.target.value))}
                    className="font-mono text-sm"
                  />
                </div>

                {/* Deadline (relative: hours + minutes from now) */}
                <div className="space-y-1.5">
                  <Label htmlFor="deadlineHours" className="text-xs">
                    Deadline (from now)
                    <FieldHint text="Soft deadline relative to submission time. The scheduler uses this to prioritize urgent jobs." />
                  </Label>
                  <div className="flex gap-2">
                    <div className="flex-1 relative">
                      <Input
                        id="deadlineHours"
                        type="number"
                        placeholder="0"
                        value={deadlineHours ?? ""}
                        onChange={(e) =>
                          setDeadlineHours(e.target.value === "" ? undefined : Number(e.target.value))
                        }
                        className="font-mono text-sm pr-8"
                      />
                      <span className="absolute right-2 top-1/2 -translate-y-1/2 text-xs text-muted-foreground pointer-events-none">
                        h
                      </span>
                    </div>
                    <div className="flex-1 relative">
                      <Input
                        id="deadlineMinutes"
                        type="number"
                        min={-59}
                        max={59}
                        placeholder="0"
                        value={deadlineMinutes ?? ""}
                        onChange={(e) => {
                          if (e.target.value === "") {
                            setDeadlineMinutes(undefined);
                            return;
                          }
                          const n = Number(e.target.value);
                          setDeadlineMinutes(Math.max(-59, Math.min(59, n)));
                        }}
                        className="font-mono text-sm pr-10"
                      />
                      <span className="absolute right-2 top-1/2 -translate-y-1/2 text-xs text-muted-foreground pointer-events-none">
                        min
                      </span>
                    </div>
                  </div>
                </div>
              </div>
            </div>

          <div className="flex gap-3">
            <Button type="submit" className="px-6" disabled={createJob.isPending}>
              {createJob.isPending ? "Submitting..." : "Submit Job"}
            </Button>
            <Button type="button" variant="outline" onClick={() => navigate("/jobs")}>
              Cancel
            </Button>
          </div>
        </form>
      </div>
    </TooltipProvider>
  );
}
