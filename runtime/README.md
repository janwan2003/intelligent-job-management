## 1) Build the image

```bash
docker build -t ijm-runtime:dev runtime/
```

Verify it exists:

```bash
docker images | grep ijm-runtime
```

---

## 2) Run it “by hand” with a checkpoint directory

Create a host directory that will persist checkpoints:

```bash
mkdir -p checkpoints
```

Run the container and mount that directory to `/checkpoints`:

```bash
docker run --name ijm-manual-test --rm \
  -v "$(pwd)/checkpoints:/checkpoints" \
  ijm-runtime:dev
```

What to expect:

* It prints progress logs periodically.
* It should create a checkpoint file in the mounted directory (e.g., `latest.pt`).

---

## 3) Stop it cleanly (trigger SIGTERM + checkpoint)

Open a second terminal and run:

```bash
docker stop -t 30 ijm-manual-test
```

Notes:

* `-t 30` gives it up to 30 seconds to checkpoint and exit cleanly.
* Your training code must handle SIGTERM for this to work.

---

## 4) Confirm it actually checkpointed

On the host:

```bash
ls -lah checkpoints
```

You should see something like:

* `latest.pt` (or whatever name your script uses)
* possibly a `.tmp` file briefly (if you used atomic writes)

If you want to watch it update while running:

```bash
watch -n 1 'ls -lah checkpoints'
```

More detail (timestamps):

```bash
stat checkpoints/latest.pt
```

---

## 5) Resume (run again with the same mount)

Just run the same `docker run` command again:

```bash
docker run --name ijm-manual-test --rm \
  -v "$(pwd)/checkpoints:/checkpoints" \
  ijm-runtime:dev
```

What to look for:

* Early in stdout, it should print something like “loaded checkpoint” / “resumed from step X”.
* Steps should continue from the last saved step (not restart from 0).

---
