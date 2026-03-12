import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useCreateJob } from "@/api/hooks";
import { FEATURE_SUBMIT_EXTENDED_FIELDS } from "@/config/features";
import {
  DEFAULT_IMAGE,
  DEFAULT_JOB_PRIORITY,
  DEFAULT_EPOCHS_TOTAL,
  DEFAULT_PROFILING_STEPS,
  DEFAULT_LOG_INTERVAL,
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

  const [image, setImage] = useState(DEFAULT_IMAGE);
  const [priority, setPriority] = useState(DEFAULT_JOB_PRIORITY);
  const [deadlineDate, setDeadlineDate] = useState("");
  const [deadlineTime, setDeadlineTime] = useState("23:59");
  const [profilingEpochsNo, setProfilingEpochsNo] = useState(DEFAULT_PROFILING_STEPS);
  const [epochsTotal, setEpochsTotal] = useState(DEFAULT_EPOCHS_TOTAL);
  const [logInterval, setLogInterval] = useState(DEFAULT_LOG_INTERVAL);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (!image.trim()) {
      toast.error("Docker image is required");
      return;
    }

    createJob.mutate(
      {
        image: image.trim(),
        Priority: priority,
        epochsTotal: epochsTotal,
        profilingEpochsNo: profilingEpochsNo,
        logInterval: logInterval,
        ...(deadlineDate && { deadline: `${deadlineDate}T${deadlineTime || "23:59"}:00` }),
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
                placeholder="e.g. ijm-runtime:dev"
                className="font-mono text-sm"
                required
              />
            </div>

          </div>

          {/* Training Parameters */}
          {FEATURE_SUBMIT_EXTENDED_FIELDS && (
            <div className="rounded-lg border border-border bg-card p-5 space-y-4">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Training Parameters</h2>

              <div className="grid grid-cols-2 gap-4">
                {/* Epochs Total */}
                <div className="space-y-1.5">
                  <Label htmlFor="epochsTotal" className="text-xs">
                    Total Steps
                    <FieldHint text="Passed as MAX_STEPS env var. Your training script should read os.environ.get('MAX_STEPS') to know when to stop." />
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

                {/* Profiling Steps */}
                <div className="space-y-1.5">
                  <Label htmlFor="profilingEpochsNo" className="text-xs">
                    Profiling Steps
                    <FieldHint text="Number of steps for each profiling run. Passed as MAX_STEPS during profiling. Short runs measure GPU throughput before the full training begins." />
                  </Label>
                  <Input
                    id="profilingEpochsNo"
                    type="number"
                    min={1}
                    value={profilingEpochsNo}
                    onChange={(e) => setProfilingEpochsNo(Number(e.target.value))}
                    className="font-mono text-sm"
                  />
                </div>

                {/* Log Interval */}
                <div className="space-y-1.5">
                  <Label htmlFor="logInterval" className="text-xs">
                    Log Interval
                    <FieldHint text="Steps between progress log lines. Passed as LOG_INTERVAL env var. Lower values give more frequent progress updates and better profiling accuracy." />
                  </Label>
                  <Input
                    id="logInterval"
                    type="number"
                    min={1}
                    value={logInterval}
                    onChange={(e) => setLogInterval(Number(e.target.value))}
                    className="font-mono text-sm"
                  />
                </div>
              </div>
            </div>
          )}

          {/* Scheduling */}
          {FEATURE_SUBMIT_EXTENDED_FIELDS && (
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

                {/* Deadline */}
                <div className="space-y-1.5">
                  <Label htmlFor="deadlineDate" className="text-xs">
                    Deadline
                    <FieldHint text="Soft deadline for job completion. The scheduler uses this to prioritize urgent jobs." />
                  </Label>
                  <div className="flex gap-2">
                    <Input
                      id="deadlineDate"
                      type="date"
                      value={deadlineDate}
                      onChange={(e) => setDeadlineDate(e.target.value)}
                      className="font-mono text-sm flex-1"
                    />
                    <Input
                      id="deadlineTime"
                      type="time"
                      value={deadlineTime}
                      onChange={(e) => setDeadlineTime(e.target.value)}
                      className="font-mono text-sm w-28"
                    />
                  </div>
                </div>
              </div>
            </div>
          )}

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
