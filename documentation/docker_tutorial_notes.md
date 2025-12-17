````md
# Docker Cheat Sheet 
## 1) Containers — list & status

### List running containers
```bash
docker ps
````

### List all containers (including stopped)

```bash
docker ps -a
```

### Compact table view

```bash
docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}"
```

### Filter by name

```bash
docker ps --filter "name=ijm-"
```

---

## 2) Logs (most important debugging tool)

### Show logs

```bash
docker logs <container>
```

### Follow logs live

```bash
docker logs -f <container>
```

### Last N lines

```bash
docker logs --tail 100 <container>
```

### Logs with timestamps

```bash
docker logs -t <container>
```

---

## 3) Start / stop / kill containers

### Graceful stop (SIGTERM → checkpoint → exit)

```bash
docker stop <container>
```

### Stop with custom grace period (recommended for DL jobs)

```bash
docker stop -t 30 <container>
```

### Immediate kill (SIGKILL — avoid unless debugging)

```bash
docker kill <container>
```

### Restart a container

```bash
docker restart <container>
```

---

## 4) Running containers manually

### Basic run

```bash
docker run <image>
```

### Run with name

```bash
docker run --name myjob <image>
```

### Auto-remove container on exit

```bash
docker run --rm <image>
```

### Run detached (background)

```bash
docker run -d <image>
```

### Run with volume mount (checkpointing)

```bash
docker run --rm \
  -v "$(pwd)/data/checkpoints/test:/checkpoints" \
  ijm-runtime:dev
```

### Interactive shell inside image

```bash
docker run -it --rm ijm-runtime:dev bash
```

---

## 5) Exec into running containers

### Open a shell

```bash
docker exec -it <container> bash
```

### Run a command inside container

```bash
docker exec <container> ls -lah /checkpoints
```

---

## 6) Inspect containers (deep debugging)

### Full inspection (JSON)

```bash
docker inspect <container>
```

### Common inspection fields

```bash
docker inspect -f '{{.State.Status}}' <container>
docker inspect -f '{{.State.ExitCode}}' <container>
docker inspect -f '{{.Config.Image}}' <container>
docker inspect -f '{{.Mounts}}' <container>
```

### Get container IP

```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' <container>
```

---

## 7) Images

### Build an image

```bash
docker build -t ijm-runtime:dev runtime/
```

### List images

```bash
docker images
```

### Remove image

```bash
docker rmi ijm-runtime:dev
```

### Remove dangling images

```bash
docker image prune
```

---

## 8) Volumes & disk usage

### Show Docker disk usage

```bash
docker system df
```

### Remove unused containers, networks, images

```bash
docker system prune
```

### Aggressive cleanup (DANGEROUS)

```bash
docker system prune -a
```

---

## 9) Networking

### List networks

```bash
docker network ls
```

### Inspect a network

```bash
docker network inspect <network>
```

---

## 10) Docker Compose (you will use this daily)

### Start services

```bash
docker compose up
```

### Start in background

```bash
docker compose up -d
```

### Rebuild images

```bash
docker compose up --build
```

### Stop & remove services

```bash
docker compose down
```

### View all compose logs

```bash
docker compose logs
```

### Follow logs for one service

```bash
docker compose logs -f worker
```

### Restart one service

```bash
docker compose restart worker
```

---

## 11) Signals & shutdown (critical for checkpointing)

### Send SIGTERM manually

```bash
docker kill --signal=SIGTERM <container>
```

### Send SIGINT

```bash
docker kill --signal=SIGINT <container>
```

### Verify clean exit

```bash
docker inspect -f '{{.State.ExitCode}}' <container>
```

* `0` → clean exit
* non-zero → crash / forced kill

---

## 12) Useful batch operations

### Stop all containers matching a prefix

```bash
docker ps -q --filter "name=ijm-" | xargs docker stop -t 30
```

### Remove all stopped containers

```bash
docker container prune
```

---

## 13) Checkpoint debugging (host-side)

### Watch checkpoint directory

```bash
watch -n 1 'ls -lah data/checkpoints/<job_id>'
```

### Inspect PyTorch checkpoint

```bash
python - <<'PY'
import torch
ckpt = torch.load("data/checkpoints/<job_id>/latest.pt", map_location="cpu")
print("keys:", ckpt.keys())
print("step:", ckpt.get("step"))
PY
```

---

## 14) Typical workflow (your project)

```bash
# start infra + worker
docker compose up -d

# submit job (via UI or curl)

# follow worker logs
docker compose logs -f worker

# stop job safely
docker stop -t 30 ijm-<job_id_prefix>

# verify checkpoint
ls data/checkpoints/<job_id>/
```

---

## 15) Rules of thumb

* **docker stop** → safe (SIGTERM, checkpointing)
* **docker kill** → unsafe (state lost)
* **docker logs -f** → first debugging tool
* **docker inspect** → last-resort debugging
* **Always mount `/checkpoints` for resumable jobs**

---

## 16) Quick reference (memorize these)

```bash
docker ps
docker logs -f <container>
docker stop -t 30 <container>
docker exec -it <container> bash
docker compose up --build
docker compose logs -f worker
```
