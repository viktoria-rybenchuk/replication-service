# 🌀 Distributed Replication System

A simple async message replication system using **FastAPI**, **aiohttp**, and **Docker Compose**.
One **master** service sends messages to multiple **followers**.

---

## 🚀 Run the project

```bash
docker compose up --build
```

## 🧱 Services

| Name        | Port | Role       |
|--------------|------|------------|
| `leader`     | 8000 | Master     |
| `follower-1` | 8001 | Follower 1 |
| `follower-2` | 8002 | Follower 2 |

---

## 🧩 How it works

- Master receives messages via `/messages`.
- Replicates them asynchronously to followers:
  - `w=1` → background replication
  - `w=2` → waits for one follower success
  - `w=3` → waits for all followers success

---

## ⚙️ Environment

**Environment variables for followers:**

```yaml
environment:
  - SERVICE_NAME=Follower-1
  - DELAY=5
```

## 📡 Test

**Send a message to the master:**

```bash
curl -X POST http://localhost:8000/messages \
  -H "Content-Type: application/json" \
  -d '{"content": "Hello!", "w":3}'
```

**View stored messages:**

```bash
curl http://localhost:8000/messages
curl http://localhost:8001/messages
curl http://localhost:8002/messages
```

**Stop and remove all containers, networks, and volumes:**
```bash
docker compose down -v
```
