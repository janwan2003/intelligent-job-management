import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useCreateJob, useUploadImage } from "@/api/hooks";
import { FEATURE_IMAGE_UPLOAD, FEATURE_SUBMIT_EXTENDED_FIELDS } from "@/config/features";
import { DEFAULT_IMAGE, DEFAULT_COMMAND, DEFAULT_JOB_PRIORITY } from "@/config/constants";
import { toast } from "sonner";
import { Upload } from "lucide-react";

export default function SubmitJob() {
  const navigate = useNavigate();
  const createJob = useCreateJob();
  const uploadImage = useUploadImage();

  const [image, setImage] = useState(DEFAULT_IMAGE);
  const [command, setCommand] = useState(DEFAULT_COMMAND);
  const [priority, setPriority] = useState(DEFAULT_JOB_PRIORITY);
  const [deadlineDate, setDeadlineDate] = useState("");
  const [deadlineTime, setDeadlineTime] = useState("23:59");
  const [batchSize, setBatchSize] = useState("");
  const [profilingEpochsNo, setProfilingEpochsNo] = useState("");
  const [epochsTotal, setEpochsTotal] = useState("");
  const [requiredMemoryGb, setRequiredMemoryGb] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();

    if (!image.trim()) {
      toast.error("Docker image is required");
      return;
    }

    if (!command.trim()) {
      toast.error("Command is required");
      return;
    }

    const commandArray = command.trim()
      ? command.split(" ").filter((s) => s.trim() !== "")
      : undefined;

    createJob.mutate(
      {
        image: image.trim(),
        command: commandArray,
        Priority: priority,
        ...(deadlineDate && { deadline: `${deadlineDate}T${deadlineTime || "23:59"}:00` }),
        ...(batchSize && { batchSize: Number(batchSize) }),
        ...(profilingEpochsNo && { profilingEpochsNo: Number(profilingEpochsNo) }),
        ...(epochsTotal && { epochsTotal: Number(epochsTotal) }),
        ...(requiredMemoryGb && { requiredMemoryGb: Number(requiredMemoryGb) }),
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

  const handleImageUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    uploadImage.mutate(file, {
      onSuccess: (data) => {
        setImage(data.image);
        toast.success(data.message);
      },
      onError: () => {
        toast.error("Failed to upload image");
      },
    });
  };

  return (
    <div className="space-y-6 max-w-2xl">
      <div>
        <h1 className="text-lg font-semibold text-foreground">Submit Job</h1>
        <p className="text-sm text-muted-foreground">Queue a new training task</p>
      </div>

      <form onSubmit={handleSubmit} className="space-y-5">
        {/* Required: Container & Execution */}
        <div className="rounded-lg border border-border bg-card p-5 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Container</h2>
            <span className="text-xs text-destructive">* Required</span>
          </div>

          {/* Docker Image */}
          <div className="space-y-1.5">
            <Label htmlFor="image" className="text-xs">
              Docker Image <span className="text-destructive">*</span>
            </Label>
            <div className="flex gap-2">
              <Input
                id="image"
                value={image}
                onChange={(e) => setImage(e.target.value)}
                placeholder="e.g. ijm-runtime:dev"
                className="font-mono text-sm flex-1"
                required
              />
              {FEATURE_IMAGE_UPLOAD && (
                <>
                  <Button
                    type="button"
                    variant="outline"
                    className="shrink-0"
                    disabled={uploadImage.isPending}
                    onClick={() => document.getElementById("image-upload")?.click()}
                  >
                    <Upload className="h-4 w-4 mr-1" />
                    {uploadImage.isPending ? "Uploading..." : "Upload .tar"}
                  </Button>
                  <input
                    id="image-upload"
                    type="file"
                    accept=".tar,.tar.gz,.tgz"
                    onChange={handleImageUpload}
                    disabled={uploadImage.isPending}
                    className="hidden"
                  />
                </>
              )}
            </div>
          </div>

          {/* Command */}
          <div className="space-y-1.5">
            <Label htmlFor="command" className="text-xs">
              Command <span className="text-destructive">*</span>
            </Label>
            <Input
              id="command"
              value={command}
              onChange={(e) => setCommand(e.target.value)}
              placeholder="e.g. python -u train.py"
              className="font-mono text-sm"
            />
            <p className="text-xs text-muted-foreground">
              Space-separated command and arguments.
            </p>
          </div>
        </div>

        {/* Optional: Training Parameters */}
        {FEATURE_SUBMIT_EXTENDED_FIELDS && (
          <div className="rounded-lg border border-border bg-card p-5 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Training Parameters</h2>
              <span className="text-xs text-muted-foreground">Optional</span>
            </div>

            <div className="grid grid-cols-2 gap-4">
              {/* Epochs Total */}
              <div className="space-y-1.5">
                <Label htmlFor="epochsTotal" className="text-xs">
                  Total Epochs
                </Label>
                <Input
                  id="epochsTotal"
                  type="number"
                  min={1}
                  value={epochsTotal}
                  onChange={(e) => setEpochsTotal(e.target.value)}
                  placeholder="e.g. 50"
                  className="font-mono text-sm"
                />
              </div>

              {/* Batch Size */}
              <div className="space-y-1.5">
                <Label htmlFor="batchSize" className="text-xs">
                  Batch Size
                </Label>
                <Input
                  id="batchSize"
                  type="number"
                  min={1}
                  value={batchSize}
                  onChange={(e) => setBatchSize(e.target.value)}
                  placeholder="e.g. 2048"
                  className="font-mono text-sm"
                />
              </div>

              {/* Profiling Epochs */}
              <div className="space-y-1.5">
                <Label htmlFor="profilingEpochsNo" className="text-xs">
                  Profiling Steps
                </Label>
                <Input
                  id="profilingEpochsNo"
                  type="number"
                  min={1}
                  value={profilingEpochsNo}
                  onChange={(e) => setProfilingEpochsNo(e.target.value)}
                  placeholder="default: 100"
                  className="font-mono text-sm"
                />
                <p className="text-xs text-muted-foreground">Steps per profiling run on each hardware config (default: 100)</p>
              </div>

              {/* Required VRAM */}
              <div className="space-y-1.5">
                <Label htmlFor="requiredMemoryGb" className="text-xs">
                  Required VRAM (GB)
                </Label>
                <Input
                  id="requiredMemoryGb"
                  type="number"
                  min={1}
                  value={requiredMemoryGb}
                  onChange={(e) => setRequiredMemoryGb(e.target.value)}
                  placeholder="e.g. 48"
                  className="font-mono text-sm"
                />
                <p className="text-xs text-muted-foreground">Minimum GPU memory needed for this job</p>
              </div>
            </div>
          </div>
        )}

        {/* Optional: Scheduling */}
        {FEATURE_SUBMIT_EXTENDED_FIELDS && (
          <div className="rounded-lg border border-border bg-card p-5 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Scheduling</h2>
              <span className="text-xs text-muted-foreground">Optional</span>
            </div>

            <div className="grid grid-cols-2 gap-4">
              {/* Priority */}
              <div className="space-y-1.5">
                <Label htmlFor="priority" className="text-xs">
                  Priority (1-5, 5 = highest)
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
                <p className="text-xs text-muted-foreground">Default: 3</p>
              </div>

              {/* Deadline */}
              <div className="space-y-1.5">
                <Label htmlFor="deadlineDate" className="text-xs">
                  Deadline
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
  );
}
