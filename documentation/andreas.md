# ANDREAS Reference — API, Configuration & Container Contract

**ANDREAS** = **A**rtificial intelligence trai**N**ing sche**D**uler fo**R** acc**E**ler**A**ted cluster**S**

Horizon 2020 / TETRAMAX project (June 2019 – Feb 2021) by Politecnico di Milano, 7bulls.com, and E4 Computer Engineering. This document extracts the exact API contracts, configuration, and container requirements from the three ANDREAS deliverables (D1, D2, D3) so our IJM project can match or extend them.

---

## 1. Job Submission

**Endpoint:** `POST /submit`

**Request body** (D3 p.28):

```json
{
  "job_id": "LSTM-small",
  "dockerImage": "tf2-gpu.aarch64:v1.2",
  "scriptPath": "/home/user/scripts/snapLstmOneStepSmall.py",
  "directoryToMount": "/home/user/lstmJobs/job_lstm_3",
  "Priority": 1,
  "deadline": "2021-02-28T17:09:42.411",
  "batchSize": 2048,
  "profilingEpochsNo": 2,
  "epochsTotal": 15
}
```

### Field descriptions (exact quotes from D3 p.28)

| Field | Description (D3) |
|---|---|
| `job_id` | "It is the unique ID identifying the job. If you launch the same script multiple times you should put here the same name as it allows to create additional profiling data and leads to a better job assignment." |
| `dockerImage` | "we highly encourage to use the container image developed within the ANDREAS project as base image as it contains everything that is necessary to launch the script on ARMIDA with GPU support" |
| `scriptPath` | "path to the script. Many jobs can use the same script file" |
| `directoryToMount` | "this is the job working directory required for each of the jobs. Different directories must be used for different submissions." |
| `Priority` | "priority of the job. This is an integer ranging from 1 (minimum priority) to 5 (maximum priority) and it is used by the Optimizer to determine the order of jobs execution." |
| `deadline` | "deadline of the job. Specific format needs to be used as in the example above" |
| `batchSize` | "the batch size that will be exported into the docker container as an environmental variable" |
| `profilingEpochsNo` | "number of epochs that will be used during profiling. More epochs give more accurate estimates for the job. However, this value should be lower than the total number of epochs that need to be processed" |
| `epochsTotal` | "total number of epochs for the job" |

**Note:** `job_id` is a _type_ identifier, not an instance ID. Submitting the same `job_id` multiple times triggers incremental profiling on more GPUs. Each submission is a separate _instance_.

---

## 2. All API Endpoints

ANDREAS has two APIs: the user-facing **Jobs Manager** (Java, port 8080) and the internal **Jobs Optimizer** (C++/Flask).

### 2.1 Jobs Manager API (user-facing)

#### `POST /nodes` — Cluster configuration

Informs the Jobs Manager about available nodes. Can be sent multiple times to reconfigure.

**Request body** (D3 pp.26–27):

```json
[
  {
    "id": "armida-04",
    "isForProfiling": false,
    "cost": 0.079918497015
  },
  {
    "id": "armida-06",
    "isForProfiling": true,
    "cost": 0.079918497015
  }
]
```

| Field | Description (D3 p.27) |
|---|---|
| `id` | "this is the name of the node available in the system and connected to the control unit" |
| `isForProfiling` | "set to true determines that the node is dedicated for the profiling" |
| `cost` | "this value is passed forward to the optimizer to find the optimal jobs assignment... corresponds to the cost related to the different nodes when they are idle" |

GPU type and count are **auto-detected** from nodes: "The system itself will recognize the available GPUs attached to them." (D3 p.26)

#### `POST /submit` — Job submission

See Section 1 above.

**Note:** D3 does not document response bodies for either Jobs Manager endpoint.

### 2.2 Jobs Optimizer API (internal)

#### `POST /optimizer/v1` — Compute optimal assignment

Called periodically by the Jobs Manager at the interval `optimizerCallDelay`.

**Request body** (D3 pp.9–10):

```json
{
  "jobs": {
    "LSTM-small": {
      "SubmissionTime": "%a %d %b %Y, %H.%M.%S",
      "Deadline": "%a %d %b %Y, %H.%M.%S",
      "Priority": 1,
      "Epochs": 15,
      "ProfilingData": {
        "tesla": { "1": 120.5, "2": 65.3 },
        "turing": { "1": 95.0 }
      }
    }
  },
  "nodes": {
    "armida-04": {
      "GPUtype": "tesla",
      "free_nGPUs": 2,
      "total_nGPUs": 4,
      "cost": 0.079918497015
    }
  },
  "GPUcosts": {
    "tesla": { "1": 0.121019438337, "2": 0.15983699403 },
    "turing": { "1": 0.09133542516 }
  },
  "currentScheduling": {
    "LSTM-small": {
      "expected_tardiness": 0.5,
      "nGPUs": 2,
      "NodeID": "armida-04"
    }
  },
  "currentTime": "%a %d %b %Y, %H.%M.%S",
  "method": "RG",
  "verbose": 1
}
```

**Key notes** (D3 p.10):

- "all fields except currentScheduling, method and verbosity level are mandatory."
- "If provided, the currentScheduling will be used to minimize changes in the running configuration, reducing the time required to migrate jobs between different nodes."
- "the Priority field is used by the optimizer to weigh the penalty due to deadline violations: jobs with higher priority will be charged with a higher penalty. Valid values are 1, 2, 3, 4 or 5"
- "the ProfilingData section stores information collected through profiling, concerning the execution times of each job with the available configurations, identified by a given type and number of GPUs."
- "the times to be provided in the ProfilingData section are expected to be in seconds. Vice versa, all costs should be provided in EUR/h."
- GPU counts are string keys: "Json format forces all keys to be provided as strings, therefore also the number of GPUs"
- Node `cost` = idle cost; `GPUcosts` = total cost when N GPUs are in use.

**Response (201):**

```json
{
  "estimated_cost": 1.234,
  "estimated_rescheduling_time": "%a %d %b %Y, %H.%M.%S",
  "jobs": {
    "LSTM-small": {
      "expected_tardiness": 0.5,
      "nGPUs": 2,
      "node": "armida-04"
    }
  }
}
```

**Error codes:**

| Code | Meaning |
|---|---|
| 414 | Mandatory field `jobs` missing |
| 424 | Mandatory field `nodes` missing |
| 434 | Mandatory field `currentTime` missing |
| 444 | Deadline or tardinessWeight = 0 for at least one job |
| 454 | Missing data in ExecutionTimes/ProfilingData |
| 464 | Optimizer failure |
| 474 | Mandatory field `GPUcosts` missing |
| 484 | Missing data in GPUcosts |

---

## 3. Container / Image Contract

### Base image

`tf2-gpu.aarch64:v1.2` — Ubuntu 18.04, CUDA 11.1.1, cuDNN 8.0.5, TensorFlow 2.3.0 (D2 p.7).

### Environment variables injected (D3 p.25)

| Variable | Description |
|---|---|
| `EPOCHS_DONE` | "value indicating how many epochs were already processed. In the example scripts, it provides the information on which snapshot needs to be loaded when a job is relaunched." |
| `EPOCHS_TO_BE_DONE` | "value indicating how many epochs still need to be processed." |
| `BATCH_SIZE` | "batch size defined during submission of the job." |

### Checkpoint / snapshot rules

1. **Save after every epoch.** "the job's script needs to save the trained model after each epoch." (D3 p.24)
2. **Filename must contain the epoch number and no other numbers.** "The name of this file needs to contain the number of epochs already computed (for example a correct name for a model file name is `model-1.h5`). No other numbers are allowed to be used in the names of the files (both input and output), except for the file extensions." (D3 p.25)
3. **Working directory.** "Each job instance needs to have a setup directory, dedicated for this instance only. All input files should be included inside." Input filenames must also avoid numbers. This directory is mounted into the container. "It is important that the script uses only relative paths, assuming that it will be launched inside the working directory" (D3 p.24)
4. **Resume from snapshot.** On relaunch the container reads `EPOCHS_DONE` and loads the correct checkpoint:
   ```python
   epochs_already_done = int(os.environ.get('EPOCHS_DONE'))
   model_path_to_load = 'model-' + str(epochs_already_done)
   path = model_path_to_load + '.h5'
   model = tf.keras.models.load_model(path)
   ```
5. **Training loop saves each epoch:**
   ```python
   for i in range(1, epochs_to_be_done + 1):
       model.fit(x_train, y_train, epochs=1, batch_size=batch_size,
                 shuffle=True, validation_data=(x_test, y_test))
       model_path_to_save = 'model-' + str(epochs_already_done + i)
       model.save(model_path_to_save + '.h5')
   ```
6. **Script is executed with:** `python3 <scriptPath>` from inside the mounted working directory.

### SIGTERM handling

**The documents do NOT describe an explicit SIGTERM contract.** ANDREAS relies on epoch-level checkpointing — the model is saved after every epoch, and the Jobs Manager tracks `EPOCHS_DONE`. When a job is preempted, the container is killed (mechanism unspecified — possibly `docker stop` or SLURM cancel), and at most one epoch of partial work is lost. The container does **not** need to handle SIGTERM gracefully.

This is a key difference from IJM, where we send SIGTERM and expect the container to checkpoint and exit cleanly.

---

## 4. System Configuration

### 4.1 `application.properties` (D3 pp.23–24)

```properties
gpuEnergyCosts = {\
 'tesla':{\
  '1':'0.121019438337',\
  '2':'0.15983699403'\
 },\
 'turing':{\
  '1':'0.09133542516'\
 }\
}

logging.file.name=jobsManager.log
optimizerCallDelay=10000
#EDF (Earliest Deadline First) or RG (Randomized Greedy)
method=EDF
```

| Field | Description |
|---|---|
| `gpuEnergyCosts` | Map of GPU type → number of GPUs → energy cost in EUR/h. Populates the `GPUcosts` field sent to the Optimizer. |
| `logging.file.name` | Log file path. |
| `optimizerCallDelay` | Milliseconds between periodic optimizer invocations. |
| `method` | Scheduling algorithm: `EDF` (Earliest Deadline First) or `RG` (Randomized Greedy). |

### 4.2 Required infrastructure (D3 p.26)

Two Docker images on the control node:
- `ttx_optimizer:v5_arm` (the optimizer)
- `mariadb:latest` (the database)

Startup scripts:
- `./startNew.sh` — deletes and restarts optimizer + DB, kills running job containers, clears profiling data.
- `./startAndContinue.sh` — starts only the Jobs Manager (keeps existing DB + optimizer).

### 4.3 Database schema (MariaDB)

**`job_profile`** — one row per job type:

| Column | Type | Notes |
|---|---|---|
| `job_id` (PK) | varchar(10) | Same as submission `job_id` |
| `default_tardiness_weight` | double | |
| `n_gpus_for_profiling` | int | Tracks how many GPUs have been profiled |

**`job_instance`** — one row per submission:

| Column | Type | Notes |
|---|---|---|
| `job_instance_id` (PK) | bigint(20) | Auto-generated |
| `job_profile_id` (FK) | varchar(10) | → `job_profile.job_id` |
| `submission_time` | datetime(6) | |
| `deadline` | datetime(6) | |
| `tardinessWeight` | double | Derived from Priority |
| `iterations_done` | int(11) | Epoch progress |
| `iterations_total` | int(11) | = `epochsTotal` |
| `status` | varchar(32) | Values not enumerated in docs |

**`job_execution_time`** — profiling results:

| Column | Type |
|---|---|
| `job_instance_id` (PK, FK) | bigint(20) |
| `gpu_id` (PK, FK) | bigint(20) |
| `n_gpus` (PK) | int(11) |
| `time` | double |

**`node`**:

| Column | Type |
|---|---|
| `id` (PK) | varchar(10) |
| `gpu_id` (FK) | bigint(20) |
| `n_gpus` | int |
| `cost` | decimal |

**`gpu`**:

| Column | Type |
|---|---|
| `id` (PK) | bigint(20) |
| `gpu_type` | varchar(10) |

---

## 5. Other Important Details

### 5.1 Profiling approach

> "every time a new job is submitted, it is executed for a given number of epochs, collecting the corresponding execution time. Each job is characterized by an ID, which is related to its type. The first time the job is submitted, it is profiled on one GPU. Then, the number of GPUs is increased every time an analogous job with the same ID is submitted, in order to increase the catalogue of execution times available for that type of job." (D3 p.14)

- Profiling runs on a **dedicated profiling node** (`isForProfiling: true`).
- Profiling is **incremental** — first submission profiles on 1 GPU, second on 2, etc.
- Profiling epoch count is per-submission (`profilingEpochsNo`).

### 5.2 Scheduling algorithm

Two methods available (D3 p.11):

**EDF (Earliest Deadline First):**
> "proceeds by sorting jobs according to their deadline, so that those that are most likely to violate it, receive resources as first. The available resources (in terms of nodes and GPUs) are partitioned among the jobs and each node can be shared by multiple applications. Once EDF allocates resources to a job, the job will continue to run without any change in the configuration until it will finish."

**Randomized Greedy (RG):**
> "a more advanced heuristic algorithm that considers jobs in increasing order of pressure. This quantity measures how much jobs are close to their deadline when executed with the fastest configuration (type and number of GPUs), so that jobs that are more likely to violate the deadline (thus incurring a penalty cost) receive resources as first."

Includes path relinking (D2 pp.8–11) and an analyzer that minimizes job migrations (D3 p.11).

### 5.3 Periodic re-optimization

> "Steps 7-9 are repeated periodically so that the optimal jobs assignment is identified in any point in time." (D3 p.8)

The optimizer is called every `optimizerCallDelay` ms (default 10000 = 10s). When the optimizer returns a different assignment, the Jobs Manager migrates jobs accordingly.

### 5.4 Technology stack

| Component | Technology |
|---|---|
| Jobs Manager | Java |
| Jobs Optimizer | C++ with Flask REST wrapper |
| Database | MariaDB |
| Container runtime | Docker |
| Resource manager | SLURM (`srun --gres=gpu:tesla:1 --nodelist=armida-06 docker run ...`) |
| Target hardware | ARMIDA cluster (ARM aarch64, Nvidia Tesla V100 32GB) |

### 5.5 Architecture evolution (D1 → D2 → D3)

- **D1:** External job queue (SLURM) → Jobs Manager → Optimizer → GPUs
- **D2:** Queue replaced by Jobs Manager REST API. "The key change of the architecture is the design and implementation of the API for submitting jobs in the Jobs manager component, instead of previously assumed usage of an external queueing system." (D2 p.4)
- **D3:** Added stop/restart/migration: "The key changes provided in the last release of the platform are the ability to stop and restart jobs from a snapshot and migrating jobs across different GPUs" (D3 p.5)

---

## 6. Gaps & Uncertainties

| Item | Status |
|---|---|
| Response bodies for `/nodes` and `/submit` | **Not documented** in any deliverable |
| Exact job status enum values | `status` is varchar(32) but valid values never listed |
| SIGTERM / stop mechanism | Documents say jobs can be "stopped" but don't specify the signal/command used |
| Authentication | No mention of any auth mechanism |
| NATS / event bus | ANDREAS does **not** use any event bus — purely REST + periodic polling |
| Port for optimizer API | Not explicitly stated (only Jobs Manager on 8080) |
| Container GPU visibility | Auto-detected by system, not documented how (`nvidia-smi`? SLURM `--gres`?) |
