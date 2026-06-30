# GPUspb optimizer calls — e2e report run 2026-06-25

Every `POST /optimizer/v5` round the IJM backend made during the two
end-to-end report scenarios, extracted verbatim from each run's
`api.log` (captured with `OPTIMIZER_VERBOSE=1`). For each call:

- **OPT REQ** — request payload: per-node free GPUs, `currentScheduling`
  (already-placed jobs), and the jobs sent as `(id[:8], priority, remaining-epochs, profiled-GPU-types)`.
- **POST** — the HTTP request to the optimizer service.
- **OPT RESP** — raw optimizer response: `estimated_cost` and the per-job placements.
- **OPT CLASSIFY** — how IJM classified each returned placement (`new` / `migrate` / `kept-same` / `drop-preempt`).
- **Optimizer: …** — the headline outcome (assignments, preemptions, cost), plus any
  deadline-miss warnings and `wants to preempt` lines.

The **→** line under each call is a one-sentence reading of what happened
and why, cross-referenced to the decision tables in
`documentation/report/Files/e2e.tex`. Job-UUID ↔ report-label map:

- **s1:** `fa2f78b3`=JOB1 (p4), `fe8f9043`=JOB2 (p4), `63cf0155`=JOB3 (p2), `15afd4f3`=JOB4 (p1), `2a474f42`=URGENT (p5)
- **s2:** `fbf0d995`=PIN-A (p5), `0e8ac2f9`=PIN-B (p5), `a27da444`=P-LSTM-1 (p1), `fce852e8`=P-LSTM-2 (p1), `8b9367c2`=URGENT (p5)

Nodes: `matemagician` = 2×QuadroP600, `polimi-gpu` = 2×A40.

## Scenario 1 — single-GPU placement (cnn_big on A40 vs P600)

`s1/api.log` — 17 optimizer calls.

### s1 · call 1  (17:01:19.479)

```
17:01:19.479 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[] | jobs=[('63cf0155', 2, '50', ['A40']), ('15afd4f3', 1, '25', ['A40'])]
17:01:19.497 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:01:19.498 INFO src.optimizer: OPT RESP cost=0.0000 jobs={}
17:01:19.498 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=[] drop-preempt=[]
17:01:19.498 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.00
```

**→** JOB3 (prio-2) and JOB4 (prio-1) are profiled and standard-ready, but both can only run on A40 — still occupied by JOB2's in-flight A40 profile sweep — so the optimizer finds no free slot and returns an empty plan; the two jobs stay queued.

### s1 · call 2  (17:01:24.431)

```
17:01:24.431 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[] | jobs=[('63cf0155', 2, '50', ['A40']), ('15afd4f3', 1, '25', ['A40'])]
17:01:24.451 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:01:24.451 INFO src.optimizer: OPT RESP cost=0.0000 jobs={}
17:01:24.451 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=[] drop-preempt=[]
17:01:24.451 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.00
```

**→** Five seconds later the A40 is still busy sweeping, so the re-run again places nothing.

### s1 · call 3  (17:01:49.980)

```
17:01:49.980 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[] | jobs=[('fe8f9043', 4, '72', ['A40']), ('63cf0155', 2, '50', ['A40']), ('15afd4f3', 1, '25', ['A40'])]
17:01:50.004 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:01:50.004 INFO src.optimizer: OPT RESP cost=0.0194 jobs={'15afd4f3-a404-4334-84c3-cd90821b7aee': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, '63cf0155-f90f-4165-a124-ca2ac9be4ff6': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:01:50.004 INFO src.optimizer: OPT CLASSIFY new=['15afd4f3', '63cf0155'] migrate=[] kept-same=[] drop-preempt=[]
17:01:50.004 INFO src.optimizer: Optimizer: 2 assignment(s), 0 preemption(s), cost=0.02
```

**→** With the A40 sweep finished, the optimizer makes the first standard placements, putting prio-2 JOB3 and prio-1 JOB4 on one A40 GPU each — the cheapest energy bundle that still meets their loose 2-hour deadlines.

### s1 · call 4  (17:01:54.948)

```
17:01:54.948 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('63cf0155', 'polimi-gpu', 'A40', 1), ('15afd4f3', 'polimi-gpu', 'A40', 1)] | jobs=[('fe8f9043', 4, '72', ['A40']), ('63cf0155', 2, '50', ['A40']), ('15afd4f3', 1, '25', ['A40'])]
17:01:54.974 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:01:54.974 INFO src.optimizer: OPT RESP cost=0.0194 jobs={'15afd4f3-a404-4334-84c3-cd90821b7aee': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, '63cf0155-f90f-4165-a124-ca2ac9be4ff6': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:01:54.974 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['15afd4f3', '63cf0155'] drop-preempt=[]
17:01:54.974 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** A heartbeat re-run confirms JOB3 and JOB4 are already on their cost-optimal slots, so nothing moves.

### s1 · call 5  (17:04:27.280)

```
17:04:27.280 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('63cf0155', 'polimi-gpu', 'A40', 1)] | jobs=[('fe8f9043', 4, '72', ['A40']), ('63cf0155', 2, '24', ['A40'])]
17:04:27.303 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:04:27.304 INFO src.optimizer: OPT RESP cost=0.0186 jobs={'63cf0155-f90f-4165-a124-ca2ac9be4ff6': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:04:27.304 INFO src.optimizer: OPT CLASSIFY new=['fe8f9043'] migrate=[] kept-same=['63cf0155'] drop-preempt=[]
17:04:27.304 INFO src.optimizer: Optimizer: 1 assignment(s), 0 preemption(s), cost=0.02
```

**→** JOB4 has finished (25/25) and JOB2's own profile sweep just completed, so the optimizer fills the freed A40 with prio-4 JOB2 — no eviction needed.

### s1 · call 6  (17:04:32.282)

```
17:04:32.282 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fe8f9043', 'polimi-gpu', 'A40', 1), ('63cf0155', 'polimi-gpu', 'A40', 1)] | jobs=[('fe8f9043', 4, '72', ['A40']), ('63cf0155', 2, '23', ['A40'])]
17:04:32.306 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:04:32.307 INFO src.optimizer: OPT RESP cost=0.0179 jobs={'63cf0155-f90f-4165-a124-ca2ac9be4ff6': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:04:32.307 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['63cf0155', 'fe8f9043'] drop-preempt=[]
17:04:32.307 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Routine heartbeat: JOB3 and JOB2 stay put, and the plan cost edges down as the next finish (the horizon Δt the proxy bills to) draws closer.

### s1 · call 7  (17:04:37.285)

```
17:04:37.285 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fe8f9043', 'polimi-gpu', 'A40', 1), ('63cf0155', 'polimi-gpu', 'A40', 1)] | jobs=[('fe8f9043', 4, '72', ['A40', 'QuadroP600']), ('63cf0155', 2, '22', ['A40', 'QuadroP600'])]
17:04:37.318 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:04:37.319 INFO src.optimizer: OPT RESP cost=0.0171 jobs={'63cf0155-f90f-4165-a124-ca2ac9be4ff6': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:04:37.319 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['63cf0155', 'fe8f9043'] drop-preempt=[]
17:04:37.319 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Another heartbeat with no change; cost keeps shrinking with the closing horizon.

### s1 · call 8  (17:04:42.301)

```
17:04:42.301 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fe8f9043', 'polimi-gpu', 'A40', 1), ('63cf0155', 'polimi-gpu', 'A40', 1)] | jobs=[('fe8f9043', 4, '74', ['A40', 'QuadroP600']), ('63cf0155', 2, '21', ['A40', 'QuadroP600'])]
17:04:42.325 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:04:42.326 INFO src.optimizer: OPT RESP cost=0.0163 jobs={'63cf0155-f90f-4165-a124-ca2ac9be4ff6': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:04:42.326 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['63cf0155', 'fe8f9043'] drop-preempt=[]
17:04:42.326 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Same again — both jobs held in place, cost ticking down.

### s1 · call 9  (17:04:47.310)

```
17:04:47.310 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fe8f9043', 'polimi-gpu', 'A40', 1), ('63cf0155', 'polimi-gpu', 'A40', 1)] | jobs=[('fe8f9043', 4, '73', ['A40', 'QuadroP600']), ('63cf0155', 2, '20', ['A40', 'QuadroP600'])]
17:04:47.335 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:04:47.335 INFO src.optimizer: OPT RESP cost=0.0155 jobs={'63cf0155-f90f-4165-a124-ca2ac9be4ff6': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:04:47.335 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['63cf0155', 'fe8f9043'] drop-preempt=[]
17:04:47.335 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Final steady-state re-run before JOB3 finishes; still no change.

### s1 · call 10  (17:06:43.126)

```
17:06:43.126 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fe8f9043', 'polimi-gpu', 'A40', 1)] | jobs=[('fe8f9043', 4, '52', ['A40', 'QuadroP600'])]
17:06:43.145 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:06:43.146 INFO src.optimizer: OPT RESP cost=0.0220 jobs={'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:06:43.146 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['fe8f9043'] drop-preempt=[]
17:06:43.146 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** JOB3 has finished (50/50), leaving only JOB2 on A40×1, which the optimizer keeps where it is.

### s1 · call 11  (17:07:07.393)

```
17:07:07.393 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fe8f9043', 'polimi-gpu', 'A40', 1)] | jobs=[('fa2f78b3', 4, '72', ['A40', 'QuadroP600']), ('fe8f9043', 4, '48', ['A40', 'QuadroP600'])]
17:07:07.424 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:07:07.425 INFO src.optimizer: OPT RESP cost=0.0373 jobs={'fa2f78b3-bea9-46a0-bd84-f5b73e18833d': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:07:07.425 INFO src.optimizer: OPT CLASSIFY new=['fa2f78b3'] migrate=[] kept-same=['fe8f9043'] drop-preempt=[]
17:07:07.425 INFO src.optimizer: Optimizer: 1 assignment(s), 0 preemption(s), cost=0.04
```

**→** JOB1's profile sweep has finished, making it a standard candidate; the optimizer places prio-4 JOB1 on the now-free second A40, so both prio-4 patients run on A40×1.

### s1 · call 12  (17:07:12.351)

```
17:07:12.351 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fa2f78b3', 'polimi-gpu', 'A40', 1), ('fe8f9043', 'polimi-gpu', 'A40', 1)] | jobs=[('fa2f78b3', 4, '72', ['A40', 'QuadroP600']), ('fe8f9043', 4, '47', ['A40', 'QuadroP600'])]
17:07:12.375 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:07:12.376 INFO src.optimizer: OPT RESP cost=0.0365 jobs={'fa2f78b3-bea9-46a0-bd84-f5b73e18833d': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:07:12.376 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['fa2f78b3', 'fe8f9043'] drop-preempt=[]
17:07:12.376 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.04
```

**→** Heartbeat confirms JOB1 and JOB2 are optimally placed; nothing changes.

### s1 · call 13  (17:08:16.627)

```
17:08:16.627 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fa2f78b3', 'polimi-gpu', 'A40', 1), ('fe8f9043', 'polimi-gpu', 'A40', 1)] | jobs=[('fa2f78b3', 4, '64', ['A40', 'QuadroP600']), ('fe8f9043', 4, '35', ['A40', 'QuadroP600']), ('2a474f42', 5, '200', ['A40', 'QuadroP600'])]
17:08:16.652 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:08:16.653 INFO src.optimizer: OPT RESP cost=0.0299 jobs={'2a474f42-a8f8-4b8c-870d-3292ffa8e1f9': {'expected_tardiness': 0.0, 'nGPUs': 2, 'node': 'matemagician'}, 'fa2f78b3-bea9-46a0-bd84-f5b73e18833d': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fe8f9043-1fc4-4a2d-9403-859ff64ec2ca': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:08:16.653 INFO src.optimizer: OPT CLASSIFY new=['2a474f42'] migrate=[] kept-same=['fa2f78b3', 'fe8f9043'] drop-preempt=[]
17:08:16.653 INFO src.optimizer: Optimizer: 1 assignment(s), 0 preemption(s), cost=0.03
```

**→** URGENT (prio-5, 200 ep, +10 min) is submitted; with the whole P600 node idle, the horizon-myopic proxy parks it on the free P600×2 for $0.03 rather than evicting a patient for faster A40 — at the next-event horizon URGENT's deadline overrun reads as zero tardiness.

### s1 · call 14  (17:11:24.158)

```
17:11:24.158 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fa2f78b3', 'polimi-gpu', 'A40', 1), ('2a474f42', 'matemagician', 'QuadroP600', 2)] | jobs=[('fa2f78b3', 4, '29', ['A40', 'QuadroP600']), ('2a474f42', 5, '196', ['A40', 'QuadroP600'])]
17:11:24.187 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:11:24.188 INFO src.optimizer: OPT RESP cost=0.0225 jobs={'2a474f42-a8f8-4b8c-870d-3292ffa8e1f9': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fa2f78b3-bea9-46a0-bd84-f5b73e18833d': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:11:24.188 INFO src.optimizer: OPT CLASSIFY new=[] migrate=['2a474f42'] kept-same=['fa2f78b3'] drop-preempt=[]
17:11:24.188 INFO src.optimizer: Optimizer: 1 assignment(s), 1 preemption(s), cost=0.02
17:11:24.188 INFO src.optimizer: Optimizer wants to preempt: ['2a474f42']
```

**→** JOB2 has finished (75/75), freeing an A40, so the optimizer migrates URGENT off the slow P600×2 onto the faster A40×1 (a stop-and-replace of URGENT itself, not an eviction of any patient).

### s1 · call 15  (17:11:29.648)

```
17:11:29.648 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fa2f78b3', 'polimi-gpu', 'A40', 1), ('2a474f42', 'polimi-gpu', 'A40', 1)] | jobs=[('fa2f78b3', 4, '28', ['A40', 'QuadroP600']), ('2a474f42', 5, '196', ['A40', 'QuadroP600'])]
17:11:29.673 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:11:29.674 INFO src.optimizer: OPT RESP cost=0.0217 jobs={'2a474f42-a8f8-4b8c-870d-3292ffa8e1f9': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fa2f78b3-bea9-46a0-bd84-f5b73e18833d': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:11:29.674 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['2a474f42', 'fa2f78b3'] drop-preempt=[]
17:11:29.674 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Heartbeat leaves URGENT on A40×1 and JOB1 on the other A40 untouched.

### s1 · call 16  (17:11:44.673)

```
17:11:44.673 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fa2f78b3', 'polimi-gpu', 'A40', 1), ('2a474f42', 'polimi-gpu', 'A40', 1)] | jobs=[('fa2f78b3', 4, '25', ['A40', 'QuadroP600']), ('2a474f42', 5, '196', ['A40', 'QuadroP600'])]
17:11:44.701 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:11:44.702 INFO src.optimizer: OPT RESP cost=0.0194 jobs={'2a474f42-a8f8-4b8c-870d-3292ffa8e1f9': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fa2f78b3-bea9-46a0-bd84-f5b73e18833d': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:11:44.702 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['2a474f42', 'fa2f78b3'] drop-preempt=[]
17:11:44.702 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Another no-change heartbeat in the same configuration.

### s1 · call 17  (17:14:02.454)

```
17:14:02.454 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('2a474f42', 'polimi-gpu', 'A40', 1)] | jobs=[('2a474f42', 5, '170', ['A40', 'QuadroP600'])]
17:14:02.474 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:14:02.474 INFO src.optimizer: OPT RESP cost=0.3493 jobs={'2a474f42-a8f8-4b8c-870d-3292ffa8e1f9': {'expected_tardiness': 0.14454166666666668, 'nGPUs': 2, 'node': 'polimi-gpu'}}
17:14:02.474 WARNING src.optimizer: Job 2a474f42 will miss deadline by 0.1 hours
17:14:02.474 INFO src.optimizer: OPT CLASSIFY new=[] migrate=['2a474f42'] kept-same=[] drop-preempt=[]
17:14:02.474 INFO src.optimizer: Optimizer: 1 assignment(s), 1 preemption(s), cost=0.35
17:14:02.474 INFO src.optimizer: Optimizer wants to preempt: ['2a474f42']
```

**→** JOB1 has finished (75/75), freeing the last A40, so the optimizer migrates URGENT onto A40×2 to finish it fastest; URGENT now sets the horizon, finally exposing its own tardiness — hence the deadline-miss warning and the cost jump to $0.35.

## Scenario 2 — 2-GPU placement + URGENT preemption (cnn_big)

`s2/api.log` — 20 optimizer calls.

### s2 · call 1  (17:31:47.763)

```
17:31:47.763 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[] | jobs=[('0e8ac2f9', 5, '77', ['A40'])]
17:31:47.779 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:31:47.779 INFO src.optimizer: OPT RESP cost=0.0000 jobs={}
17:31:47.779 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=[] drop-preempt=[]
17:31:47.779 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.00
```

**→** PIN-B (prio-5) is the only standard-ready job and needs A40, still occupied by P-LSTM-2's in-flight A40 profile sweep, so the optimizer returns an empty plan and PIN-B waits.

### s2 · call 2  (17:31:52.733)

```
17:31:52.733 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[] | jobs=[('0e8ac2f9', 5, '77', ['A40'])]
17:31:52.748 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:31:52.749 INFO src.optimizer: OPT RESP cost=0.0000 jobs={}
17:31:52.749 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=[] drop-preempt=[]
17:31:52.749 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.00
```

**→** Five seconds later the A40 is still sweeping, so nothing is placed again.

### s2 · call 3  (17:32:30.141)

```
17:32:30.141 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[] | jobs=[('0e8ac2f9', 5, '77', ['A40']), ('fce852e8', 1, '197', ['QuadroP600', 'A40'])]
17:32:30.161 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:32:30.162 INFO src.optimizer: OPT RESP cost=0.0603 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:32:30.162 INFO src.optimizer: OPT CLASSIFY new=['0e8ac2f9', 'fce852e8'] migrate=[] kept-same=[] drop-preempt=[]
17:32:30.162 INFO src.optimizer: Optimizer: 2 assignment(s), 0 preemption(s), cost=0.06
```

**→** Their profile budgets met, the optimizer makes the first placements: prio-5 PIN-B and prio-1 P-LSTM-2 each take an A40 GPU (the only bundles profiled so far).

### s2 · call 4  (17:32:35.099)

```
17:32:35.099 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('fce852e8', 'polimi-gpu', 'A40', 1)] | jobs=[('0e8ac2f9', 5, '77', ['A40']), ('fce852e8', 1, '197', ['QuadroP600', 'A40'])]
17:32:35.124 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:32:35.124 INFO src.optimizer: OPT RESP cost=0.0603 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:32:35.124 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['0e8ac2f9', 'fce852e8'] drop-preempt=[]
17:32:35.124 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.06
```

**→** A heartbeat re-run keeps PIN-B and P-LSTM-2 on A40×1; no change.

### s2 · call 5  (17:32:40.121)

```
17:32:40.121 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('fce852e8', 'polimi-gpu', 'A40', 1)] | jobs=[('0e8ac2f9', 5, '77', ['A40']), ('fce852e8', 1, '197', ['QuadroP600', 'A40'])]
17:32:40.145 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:32:40.145 INFO src.optimizer: OPT RESP cost=0.0603 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:32:40.146 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['0e8ac2f9', 'fce852e8'] drop-preempt=[]
17:32:40.146 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.06
```

**→** Another no-change heartbeat in the same layout.

### s2 · call 6  (17:32:46.114)

```
17:32:46.114 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('fce852e8', 'polimi-gpu', 'A40', 1)] | jobs=[('0e8ac2f9', 5, '79', ['A40']), ('fce852e8', 1, '199', ['QuadroP600', 'A40'])]
17:32:46.138 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:32:46.139 INFO src.optimizer: OPT RESP cost=0.0619 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:32:46.139 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['0e8ac2f9', 'fce852e8'] drop-preempt=[]
17:32:46.139 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.06
```

**→** Same again — both held on A40, cost roughly flat.

### s2 · call 7  (17:34:30.907)

```
17:34:30.907 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('fce852e8', 'polimi-gpu', 'A40', 1)] | jobs=[('0e8ac2f9', 5, '60', ['A40', 'QuadroP600']), ('fce852e8', 1, '182', ['QuadroP600', 'A40'])]
17:34:30.932 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:34:30.932 INFO src.optimizer: OPT RESP cost=0.0470 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:34:30.932 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['0e8ac2f9', 'fce852e8'] drop-preempt=[]
17:34:30.932 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.05
```

**→** Steady-state re-run; still PIN-B and P-LSTM-2 on A40, cost drifting down with the horizon.

### s2 · call 8  (17:34:35.859)

```
17:34:35.859 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('fce852e8', 'polimi-gpu', 'A40', 1)] | jobs=[('0e8ac2f9', 5, '59', ['A40', 'QuadroP600']), ('fce852e8', 1, '181', ['QuadroP600', 'A40'])]
17:34:35.883 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:34:35.884 INFO src.optimizer: OPT RESP cost=0.0462 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:34:35.884 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['0e8ac2f9', 'fce852e8'] drop-preempt=[]
17:34:35.884 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.05
```

**→** Last heartbeat before PIN-A's sweep completes; no change.

### s2 · call 9  (17:37:02.590)

```
17:37:02.590 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('fce852e8', 'polimi-gpu', 'A40', 1)] | jobs=[('fbf0d995', 5, '77', ['A40', 'QuadroP600']), ('0e8ac2f9', 5, '32', ['A40', 'QuadroP600']), ('fce852e8', 1, '156', ['QuadroP600', 'A40'])]
17:37:02.618 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:37:02.619 INFO src.optimizer: OPT RESP cost=0.0251 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:37:02.619 INFO src.optimizer: OPT CLASSIFY new=['fbf0d995'] migrate=[] kept-same=['0e8ac2f9'] drop-preempt=['fce852e8']
17:37:02.619 INFO src.optimizer: Optimizer: 1 assignment(s), 1 preemption(s), cost=0.03
17:37:02.619 INFO src.optimizer: Optimizer wants to preempt: ['fce852e8']
```

**→** PIN-A's A40 sweep has finished; the optimizer prefers prio-5 PIN-A over prio-1 P-LSTM-2 for the scarce A40, so it drop-preempts (evicts) P-LSTM-2 and gives PIN-A the A40×1 slot.

### s2 · call 10  (17:37:07.543)

```
17:37:07.543 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('0e8ac2f9', 'polimi-gpu', 'A40', 1)] | jobs=[('fbf0d995', 5, '77', ['A40', 'QuadroP600']), ('0e8ac2f9', 5, '31', ['A40', 'QuadroP600']), ('fce852e8', 1, '155', ['QuadroP600', 'A40'])]
17:37:07.572 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:37:07.573 INFO src.optimizer: OPT RESP cost=0.0243 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:37:07.573 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['0e8ac2f9', 'fbf0d995'] drop-preempt=[]
17:37:07.573 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Heartbeat confirms the two prio-5 pins (PIN-A, PIN-B) hold both A40 GPUs; nothing moves.

### s2 · call 11  (17:38:06.381)

```
17:38:06.381 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('0e8ac2f9', 'polimi-gpu', 'A40', 1)] | jobs=[('fbf0d995', 5, '74', ['A40', 'QuadroP600']), ('0e8ac2f9', 5, '20', ['A40', 'QuadroP600']), ('a27da444', 1, '197', ['QuadroP600', 'A40']), ('fce852e8', 1, '154', ['QuadroP600', 'A40'])]
17:38:06.415 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:38:06.415 INFO src.optimizer: OPT RESP cost=0.0172 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
17:38:06.415 INFO src.optimizer: OPT CLASSIFY new=['a27da444', 'fce852e8'] migrate=[] kept-same=['0e8ac2f9', 'fbf0d995'] drop-preempt=[]
17:38:06.415 INFO src.optimizer: Optimizer: 2 assignment(s), 0 preemption(s), cost=0.02
```

**→** The optimizer places P-LSTM-1 and the just-evicted P-LSTM-2 on a P600 GPU each; the cluster is now full — A40 = two prio-5 pins, P600 = two prio-1 lstm-small.

### s2 · call 12  (17:38:11.330)

```
17:38:11.330 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('a27da444', 'matemagician', 'QuadroP600', 1), ('fce852e8', 'matemagician', 'QuadroP600', 1)] | jobs=[('fbf0d995', 5, '73', ['A40', 'QuadroP600']), ('0e8ac2f9', 5, '19', ['A40', 'QuadroP600']), ('a27da444', 1, '197', ['QuadroP600', 'A40']), ('fce852e8', 1, '154', ['QuadroP600', 'A40']), ('8b9367c2', 5, '40', ['A40', 'QuadroP600'])]
17:38:11.365 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:38:11.366 INFO src.optimizer: OPT RESP cost=0.0164 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, '8b9367c2-49d6-4145-9468-3e313bdb77ee': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:38:11.366 INFO src.optimizer: OPT CLASSIFY new=['8b9367c2'] migrate=[] kept-same=['0e8ac2f9', 'a27da444', 'fbf0d995'] drop-preempt=['fce852e8']
17:38:11.366 INFO src.optimizer: Optimizer: 1 assignment(s), 1 preemption(s), cost=0.02
17:38:11.366 INFO src.optimizer: Optimizer wants to preempt: ['fce852e8']
```

**→** URGENT (prio-5, 40 ep, +22 min) arrives; the deadline-meeting A40 bundles are held by equal-priority pins it cannot preempt and no P600 bundle reaches the deadline, so the proxy takes the cheapest preemptible bundle — P600×1 — evicting one prio-1 lstm (P-LSTM-2).

### s2 · call 13  (17:38:16.329)

```
17:38:16.329 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('0e8ac2f9', 'polimi-gpu', 'A40', 1), ('a27da444', 'matemagician', 'QuadroP600', 1), ('8b9367c2', 'matemagician', 'QuadroP600', 1)] | jobs=[('fbf0d995', 5, '72', ['A40', 'QuadroP600']), ('0e8ac2f9', 5, '18', ['A40', 'QuadroP600']), ('a27da444', 1, '199', ['QuadroP600', 'A40']), ('fce852e8', 1, '154', ['QuadroP600', 'A40']), ('8b9367c2', 5, '40', ['A40', 'QuadroP600'])]
17:38:16.368 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:38:16.369 INFO src.optimizer: OPT RESP cost=0.0155 jobs={'0e8ac2f9-ebe4-4bb1-881a-3fc484b86967': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, '8b9367c2-49d6-4145-9468-3e313bdb77ee': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}}
17:38:16.369 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['0e8ac2f9', '8b9367c2', 'a27da444', 'fbf0d995'] drop-preempt=[]
17:38:16.369 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Heartbeat: URGENT runs on P600×1, pins on A40, P-LSTM-2 requeued — nothing changes.

### s2 · call 14  (17:39:56.180)

```
17:39:56.180 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('a27da444', 'matemagician', 'QuadroP600', 1), ('8b9367c2', 'matemagician', 'QuadroP600', 1)] | jobs=[('fbf0d995', 5, '54', ['A40', 'QuadroP600']), ('a27da444', 1, '189', ['QuadroP600', 'A40']), ('fce852e8', 1, '154', ['QuadroP600', 'A40']), ('8b9367c2', 5, '38', ['A40', 'QuadroP600'])]
17:39:56.213 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:39:56.213 INFO src.optimizer: OPT RESP cost=0.1050 jobs={'8b9367c2-49d6-4145-9468-3e313bdb77ee': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.045208333333333336, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
17:39:56.213 WARNING src.optimizer: Job fbf0d995 will miss deadline by 0.0 hours
17:39:56.213 INFO src.optimizer: OPT CLASSIFY new=['fce852e8'] migrate=['8b9367c2'] kept-same=['a27da444', 'fbf0d995'] drop-preempt=[]
17:39:56.213 INFO src.optimizer: Optimizer: 2 assignment(s), 1 preemption(s), cost=0.10
17:39:56.213 INFO src.optimizer: Optimizer wants to preempt: ['8b9367c2']
```

**→** PIN-B has finished (80/80), freeing an A40, so the optimizer migrates URGENT off P600 onto the faster A40×1 (where it will make its deadline) and resumes the evicted P-LSTM-2 on the freed P600 GPU; PIN-A, still on a single A40, is now projected to just miss its deadline.

### s2 · call 15  (17:40:01.836)

```
17:40:01.836 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('a27da444', 'matemagician', 'QuadroP600', 1), ('fce852e8', 'matemagician', 'QuadroP600', 1), ('8b9367c2', 'polimi-gpu', 'A40', 1)] | jobs=[('fbf0d995', 5, '53', ['A40', 'QuadroP600']), ('a27da444', 1, '188', ['QuadroP600', 'A40']), ('fce852e8', 1, '154', ['QuadroP600', 'A40']), ('8b9367c2', 5, '38', ['A40', 'QuadroP600'])]
17:40:01.869 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:40:01.870 INFO src.optimizer: OPT RESP cost=0.1072 jobs={'8b9367c2-49d6-4145-9468-3e313bdb77ee': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.04659722222222222, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
17:40:01.870 WARNING src.optimizer: Job fbf0d995 will miss deadline by 0.0 hours
17:40:01.870 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['8b9367c2', 'a27da444', 'fbf0d995', 'fce852e8'] drop-preempt=[]
17:40:01.870 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.11
```

**→** A no-change heartbeat; PIN-A's projected overrun is logged again.

### s2 · call 16  (17:40:16.871)

```
17:40:16.871 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('a27da444', 'matemagician', 'QuadroP600', 1), ('fce852e8', 'matemagician', 'QuadroP600', 1), ('8b9367c2', 'polimi-gpu', 'A40', 1)] | jobs=[('fbf0d995', 5, '50', ['A40', 'QuadroP600']), ('a27da444', 1, '187', ['QuadroP600', 'A40']), ('fce852e8', 1, '153', ['QuadroP600', 'A40']), ('8b9367c2', 5, '38', ['A40', 'QuadroP600'])]
17:40:16.905 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:40:16.905 INFO src.optimizer: OPT RESP cost=0.1139 jobs={'8b9367c2-49d6-4145-9468-3e313bdb77ee': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.050763888888888886, 'nGPUs': 1, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
17:40:16.905 WARNING src.optimizer: Job fbf0d995 will miss deadline by 0.1 hours
17:40:16.906 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['8b9367c2', 'a27da444', 'fbf0d995', 'fce852e8'] drop-preempt=[]
17:40:16.906 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.11
```

**→** Same layout; PIN-A's deadline-miss warning grows as it keeps running on a single A40.

### s2 · call 17  (17:43:45.292)

```
17:43:45.292 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('fbf0d995', 'polimi-gpu', 'A40', 1), ('a27da444', 'matemagician', 'QuadroP600', 1), ('fce852e8', 'matemagician', 'QuadroP600', 1)] | jobs=[('fbf0d995', 5, '11', ['A40', 'QuadroP600']), ('a27da444', 1, '164', ['QuadroP600', 'A40']), ('fce852e8', 1, '134', ['QuadroP600', 'A40'])]
17:43:45.326 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:43:45.327 INFO src.optimizer: OPT RESP cost=0.1203 jobs={'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fbf0d995-189b-4bcf-bf7c-b11290a5a920': {'expected_tardiness': 0.06960277777777778, 'nGPUs': 2, 'node': 'polimi-gpu'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
17:43:45.327 WARNING src.optimizer: Job fbf0d995 will miss deadline by 0.1 hours
17:43:45.327 INFO src.optimizer: OPT CLASSIFY new=[] migrate=['fbf0d995'] kept-same=['a27da444', 'fce852e8'] drop-preempt=[]
17:43:45.327 INFO src.optimizer: Optimizer: 1 assignment(s), 1 preemption(s), cost=0.12
17:43:45.327 INFO src.optimizer: Optimizer wants to preempt: ['fbf0d995']
```

**→** With URGENT done and the second A40 free, the optimizer migrates the late PIN-A from A40×1 onto A40×2 to finish it as fast as the hardware allows.

### s2 · call 18  (17:45:14.179)

```
17:45:14.179 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('a27da444', 'matemagician', 'QuadroP600', 1), ('fce852e8', 'matemagician', 'QuadroP600', 1)] | jobs=[('a27da444', 1, '155', ['QuadroP600', 'A40']), ('fce852e8', 1, '126', ['QuadroP600', 'A40'])]
17:45:14.205 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:45:14.205 INFO src.optimizer: OPT RESP cost=0.0177 jobs={'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
17:45:14.206 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['a27da444', 'fce852e8'] drop-preempt=[]
17:45:14.206 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.02
```

**→** Only the two prio-1 lstm-small patients remain, running to completion on P600; their far-off +8 h deadlines keep the plan cost near zero.

### s2 · call 19  (17:52:29.706)

```
17:52:29.706 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('a27da444', 'matemagician', 'QuadroP600', 1), ('fce852e8', 'matemagician', 'QuadroP600', 1)] | jobs=[('a27da444', 1, '108', ['QuadroP600', 'A40']), ('fce852e8', 1, '86', ['QuadroP600', 'A40'])]
17:52:29.730 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
17:52:29.730 INFO src.optimizer: OPT RESP cost=0.0120 jobs={'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}, 'fce852e8-9b96-4047-be18-47b6477dbba3': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
17:52:29.730 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['a27da444', 'fce852e8'] drop-preempt=[]
17:52:29.730 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.01
```

**→** A heartbeat over the same two patients; cost continues to fall toward zero.

### s2 · call 20  (18:07:48.730)

```
18:07:48.730 INFO src.optimizer: OPT REQ nodes={'matemagician': {'GPUtype': 'QuadroP600', 'total_nGPUs': 2}, 'polimi-gpu': {'GPUtype': 'A40', 'total_nGPUs': 2}} | currentSched=[('a27da444', 'matemagician', 'QuadroP600', 1)] | jobs=[('a27da444', 1, '7', ['QuadroP600', 'A40'])]
18:07:48.752 INFO httpx: HTTP Request: POST http://localhost:8080/optimizer/v5 "HTTP/1.1 201 CREATED"
18:07:48.752 INFO src.optimizer: OPT RESP cost=0.0005 jobs={'a27da444-c0d2-47f9-85e1-c69161de9281': {'expected_tardiness': 0.0, 'nGPUs': 1, 'node': 'matemagician'}}
18:07:48.752 INFO src.optimizer: OPT CLASSIFY new=[] migrate=[] kept-same=['a27da444'] drop-preempt=[]
18:07:48.752 INFO src.optimizer: Optimizer: 0 assignment(s), 0 preemption(s), cost=0.00
```

**→** Final round near the run's end: P-LSTM-1 is still finishing on P600×1, with nothing left to change.

