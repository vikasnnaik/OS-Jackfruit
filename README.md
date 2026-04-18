# Multi-Container Runtime with Kernel Memory Monitoring

## 📌 Overview

This project implements a lightweight multi-container runtime in C along with a Linux kernel module for memory monitoring. The system supports running multiple containers simultaneously with process and filesystem isolation, controlled resource usage, and supervisor-based management.

---

## ⚙️ Features

* Multi-container runtime using Linux namespaces
* Supervisor process for lifecycle management
* CLI interface for container operations
* Bounded-buffer logging system (producer-consumer model)
* Kernel module for memory monitoring
* Soft and hard memory limit enforcement
* Scheduling experiments using nice values
* Proper cleanup with no zombie processes

---

## 🛠️ Technologies Used

* C Programming
* Linux System Calls (`clone`, `chroot`, `mount`)
* POSIX Threads (pthreads)
* UNIX Domain Sockets
* Linux Kernel Module (LKM)
* Bash / Ubuntu VM

---

## 🚀 Build & Setup

### 1. Install dependencies

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

### 2. Build project

```bash
cd boilerplate
make
```

---

## 📦 Prepare Root Filesystem

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base

cp -a rootfs-base rootfs-alpha
cp -a rootfs-base rootfs-beta
```

---

## ▶️ Running the Project

### 1. Load kernel module

```bash
sudo insmod monitor.ko
ls /dev/container_monitor
```

### 2. Start supervisor

```bash
sudo ./engine supervisor ./rootfs-base
```

### 3. Start containers (new terminal)

```bash
sudo ./engine start alpha ./rootfs-alpha "sleep 100"
sudo ./engine start beta ./rootfs-beta "sleep 100"
```

### 4. View containers

```bash
sudo ./engine ps
```

### 5. View logs

```bash
sudo ./engine logs alpha
```

### 6. Stop container

```bash
sudo ./engine stop alpha
```

---

## 📊 Task Demonstrations

### ✅ Task 1: Multi-container runtime

* Multiple containers (`alpha`, `beta`) run simultaneously

### ✅ Task 2: CLI and IPC

* Commands: start, run, ps, logs, stop
* Communication via UNIX domain socket

### ✅ Task 3: Logging system

* Container output captured via pipes
* Producer-consumer model with bounded buffer

### ✅ Task 4: Memory monitoring

* Soft limit → warning
* Hard limit → container killed

```bash
dmesg | tail
```

### ✅ Task 5: Scheduling experiment

```bash
sudo ./engine start alpha ./rootfs-alpha "./cpu_hog" --nice -5
sudo ./engine start beta ./rootfs-beta "./cpu_hog" --nice 10
```

* Lower nice value → higher CPU priority

### ✅ Task 6: Cleanup

* No zombie processes
* Proper resource deallocation
* Kernel module safely unloaded

---

## 🧠 Engineering Concepts

### 🔹 Process Isolation

* Achieved using PID, UTS, and mount namespaces
* Each container has its own filesystem via `chroot()`

### 🔹 IPC & Synchronization

* UNIX socket for CLI communication
* Pipes for logging
* Mutex + condition variables for thread safety

### 🔹 Memory Management

* RSS used to measure memory usage
* Kernel enforces limits for reliability

### 🔹 Scheduling

* Linux scheduler prioritizes processes using nice values

---

## 📸 Screenshots

Add your screenshots here:

* Multi-container execution
* ps command output
* logs output
* soft/hard limit (dmesg)
* scheduling experiment
* cleanup

---

## 🧹 Cleanup

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
sudo rmmod monitor
```

---

## 📂 Repository Structure

```
boilerplate/
├── engine.c
├── monitor.c
├── monitor_ioctl.h
├── Makefile
├── cpu_hog.c
├── memory_hog.c
├── io_pulse.c
```

---

## 👨‍💻 Team Members

* Name 1 (SRN)
* Name 2 (SRN)

---

## 🔗 GitHub Repository

Add your repo link here.

---

## 📌 Conclusion

This project demonstrates key operating system concepts including containerization, process isolation, IPC, synchronization, memory management, and scheduling through a practical implementation.

---
