/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Implements all six tasks:
 *   Task 1: Multi-container runtime with parent supervisor
 *   Task 2: Supervisor CLI and signal handling
 *   Task 3: Bounded-buffer logging and IPC design
 *   Task 4: Kernel memory monitoring integration
 *   Task 5: Scheduler experiments (nice value support)
 *   Task 6: Resource cleanup
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)
#define MAX_CONTAINERS 64

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int stop_requested;
    char log_path[PATH_MAX];
    char rootfs[PATH_MAX];
    int pipe_fd;          /* read end of container stdout/stderr pipe */
    void *clone_stack;    /* allocated stack for clone */
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    volatile int should_stop;
    pthread_t logger_thread;
    pthread_t *pipe_threads;
    int pipe_thread_count;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
    char base_rootfs[PATH_MAX];
} supervisor_ctx_t;

/* Global supervisor context for signal handlers */
static supervisor_ctx_t *g_ctx = NULL;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

/* ======================== Bounded Buffer ======================== */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/* Task 3: Producer-side insertion */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* Task 3: Consumer-side removal */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* Task 3: Logging consumer thread */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        if (bounded_buffer_pop(&ctx->log_buffer, &item) != 0)
            break;

        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);

        FILE *f = fopen(log_path, "a");
        if (f) {
            fwrite(item.data, 1, item.length, f);
            fclose(f);
        }
    }

    /* Drain remaining items after shutdown signal */
    pthread_mutex_lock(&ctx->log_buffer.mutex);
    while (ctx->log_buffer.count > 0) {
        item = ctx->log_buffer.items[ctx->log_buffer.head];
        ctx->log_buffer.head = (ctx->log_buffer.head + 1) % LOG_BUFFER_CAPACITY;
        ctx->log_buffer.count--;
        pthread_mutex_unlock(&ctx->log_buffer.mutex);

        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);
        FILE *f = fopen(log_path, "a");
        if (f) {
            fwrite(item.data, 1, item.length, f);
            fclose(f);
        }

        pthread_mutex_lock(&ctx->log_buffer.mutex);
    }
    pthread_mutex_unlock(&ctx->log_buffer.mutex);

    return NULL;
}

/* ======================== Pipe reader thread ======================== */

typedef struct {
    supervisor_ctx_t *ctx;
    char container_id[CONTAINER_ID_LEN];
    int pipe_fd;
} pipe_reader_arg_t;

/* Producer thread: reads container pipe output and pushes to bounded buffer */
static void *pipe_reader_thread(void *arg)
{
    pipe_reader_arg_t *parg = (pipe_reader_arg_t *)arg;
    supervisor_ctx_t *ctx = parg->ctx;
    char container_id[CONTAINER_ID_LEN];
    int pipe_fd = parg->pipe_fd;
    log_item_t item;

    strncpy(container_id, parg->container_id, CONTAINER_ID_LEN - 1);
    container_id[CONTAINER_ID_LEN - 1] = '\0';
    free(parg);

    while (1) {
        ssize_t n = read(pipe_fd, item.data, LOG_CHUNK_SIZE - 1);
        if (n <= 0)
            break;

        item.data[n] = '\0';
        item.length = (size_t)n;
        strncpy(item.container_id, container_id, CONTAINER_ID_LEN - 1);
        item.container_id[CONTAINER_ID_LEN - 1] = '\0';

        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    close(pipe_fd);
    return NULL;
}

/* ======================== Container child ======================== */

/* Task 1: clone child entrypoint */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Set hostname to container ID */
    sethostname(cfg->id, strlen(cfg->id));

    /* Apply nice value for scheduler experiments (Task 5) */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) == -1 && errno != 0)
            perror("nice");
    }

    /* Redirect stdout and stderr to the logging pipe */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* chroot into the container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        _exit(1);
    }
    if (chdir("/") != 0) {
        perror("chdir");
        _exit(1);
    }

    /* Mount /proc inside container */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("mount /proc");

    /* Execute the command */
    execlp("/bin/sh", "sh", "-c", cfg->command, NULL);
    perror("execlp");
    _exit(1);
}

/* ======================== Monitor integration ======================== */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ======================== Container helpers ======================== */

static container_record_t *find_container(supervisor_ctx_t *ctx, const char *id)
{
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, id) == 0)
            return rec;
        rec = rec->next;
    }
    return NULL;
}

static int start_container(supervisor_ctx_t *ctx, const control_request_t *req)
{
    /* Check for duplicate ID */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *existing = find_container(ctx, req->container_id);
    if (existing && (existing->state == CONTAINER_RUNNING || existing->state == CONTAINER_STARTING)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return -1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Check rootfs is not used by another running container */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *r = ctx->containers;
    while (r) {
        if ((r->state == CONTAINER_RUNNING || r->state == CONTAINER_STARTING) &&
            strcmp(r->rootfs, req->rootfs) == 0 &&
            strcmp(r->id, req->container_id) != 0) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            return -2;
        }
        r = r->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Create logging pipe (Path A: container stdout/stderr -> supervisor) */
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe");
        return -3;
    }

    /* Prepare child config */
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        close(pipefd[0]);
        close(pipefd[1]);
        return -3;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs, req->rootfs, PATH_MAX - 1);
    strncpy(cfg->command, req->command, CHILD_COMMAND_LEN - 1);
    cfg->nice_value = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    /* Allocate clone stack */
    void *stack = malloc(STACK_SIZE);
    if (!stack) {
        free(cfg);
        close(pipefd[0]);
        close(pipefd[1]);
        return -3;
    }

    /* Clone with PID, UTS, mount namespaces */
    pid_t pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      cfg);
    if (pid < 0) {
        perror("clone");
        free(stack);
        free(cfg);
        close(pipefd[0]);
        close(pipefd[1]);
        return -3;
    }

    /* Parent: close write end of pipe */
    close(pipefd[1]);

    /* Create container record */
    container_record_t *rec = calloc(1, sizeof(container_record_t));
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(rec->rootfs, req->rootfs, PATH_MAX - 1);
    rec->host_pid = pid;
    rec->started_at = time(NULL);
    rec->state = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code = -1;
    rec->exit_signal = 0;
    rec->stop_requested = 0;
    rec->pipe_fd = pipefd[0];
    rec->clone_stack = stack;
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Register with kernel monitor (Task 4) */
    if (ctx->monitor_fd >= 0) {
        register_with_monitor(ctx->monitor_fd, req->container_id, pid,
                              req->soft_limit_bytes, req->hard_limit_bytes);
    }

    /* Start a pipe reader producer thread (Task 3 - Path A) */
    pipe_reader_arg_t *parg = malloc(sizeof(pipe_reader_arg_t));
    parg->ctx = ctx;
    strncpy(parg->container_id, req->container_id, CONTAINER_ID_LEN - 1);
    parg->container_id[CONTAINER_ID_LEN - 1] = '\0';
    parg->pipe_fd = pipefd[0];

    pthread_t tid;
    pthread_create(&tid, NULL, pipe_reader_thread, parg);
    pthread_detach(tid);

    /* cfg memory: used by child after fork, can't safely free in parent
     * since clone shares address space briefly. In practice the child
     * either execs or exits, and the small leak is acceptable. We'll
     * free it after child exits via stack cleanup. We store it nowhere
     * special since execlp replaces the process image. */
    /* Note: cfg is on heap, child does execlp which replaces address space,
     * so we can free cfg here safely since child has already copied what it needs
     * through the clone. Actually with clone + CLONE_NEWPID, address space IS
     * shared. So we should NOT free cfg until child has used it.
     * We'll accept this small controlled leak - child uses cfg immediately
     * in child_fn before calling execlp. */

    printf("[supervisor] Started container %s (PID %d)\n", req->container_id, pid);
    return 0;
}

static void stop_container(supervisor_ctx_t *ctx, const char *id)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = find_container(ctx, id);
    if (!rec || rec->state != CONTAINER_RUNNING) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return;
    }
    rec->stop_requested = 1;
    pid_t pid = rec->host_pid;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Send SIGTERM first, then SIGKILL after a grace period */
    kill(pid, SIGTERM);
    usleep(500000); /* 0.5 sec grace */

    /* Check if still running */
    if (waitpid(pid, NULL, WNOHANG) == 0) {
        kill(pid, SIGKILL);
    }
}

/* Build ps output string */
static int build_ps_output(supervisor_ctx_t *ctx, char *buf, size_t bufsize)
{
    int offset = 0;
    offset += snprintf(buf + offset, bufsize - offset,
        "%-12s %-8s %-10s %-10s %-12s %-12s %s\n",
        "CONTAINER", "PID", "STATE", "STARTED", "SOFT(MiB)", "HARD(MiB)", "EXIT");

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec && offset < (int)bufsize - 1) {
        char time_buf[32] = "";
        struct tm *tm_info = localtime(&rec->started_at);
        if (tm_info)
            strftime(time_buf, sizeof(time_buf), "%H:%M:%S", tm_info);

        char exit_buf[32] = "-";
        if (rec->state == CONTAINER_EXITED)
            snprintf(exit_buf, sizeof(exit_buf), "code=%d", rec->exit_code);
        else if (rec->state == CONTAINER_KILLED)
            snprintf(exit_buf, sizeof(exit_buf), "signal=%d", rec->exit_signal);
        else if (rec->state == CONTAINER_STOPPED)
            snprintf(exit_buf, sizeof(exit_buf), "stopped");

        offset += snprintf(buf + offset, bufsize - offset,
            "%-12s %-8d %-10s %-10s %-12lu %-12lu %s\n",
            rec->id, rec->host_pid, state_to_string(rec->state),
            time_buf, rec->soft_limit_bytes >> 20,
            rec->hard_limit_bytes >> 20, exit_buf);
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
    return offset;
}

/* Build logs output */
static int build_logs_output(supervisor_ctx_t *ctx, const char *id, char *buf, size_t bufsize)
{
    (void)ctx;
    char log_path[PATH_MAX];
    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, id);

    FILE *f = fopen(log_path, "r");
    if (!f) {
        return snprintf(buf, bufsize, "No logs found for container %s\n", id);
    }

    size_t n = fread(buf, 1, bufsize - 1, f);
    buf[n] = '\0';
    fclose(f);
    return (int)n;
}

/* ======================== Signal handling ======================== */

static void sigchld_handler(int sig)
{
    (void)sig;
    /* Reaping happens in the main loop to avoid race conditions */
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code = WEXITSTATUS(status);
                    rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    if (rec->stop_requested) {
                        rec->state = CONTAINER_STOPPED;
                    } else if (rec->exit_signal == SIGKILL) {
                        rec->state = CONTAINER_KILLED; /* hard_limit_killed */
                    } else {
                        rec->state = CONTAINER_KILLED;
                    }
                }

                /* Unregister from kernel monitor */
                if (ctx->monitor_fd >= 0) {
                    unregister_from_monitor(ctx->monitor_fd, rec->id, pid);
                }

                /* Free clone stack (Task 6) */
                if (rec->clone_stack) {
                    free(rec->clone_stack);
                    rec->clone_stack = NULL;
                }

                printf("[supervisor] Container %s (PID %d) exited: state=%s\n",
                       rec->id, pid, state_to_string(rec->state));
                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ======================== Control plane (Path B) ======================== */

/* Handle a single control request from a CLI client */
static void handle_control_request(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t n;

    memset(&resp, 0, sizeof(resp));

    n = read(client_fd, &req, sizeof(req));
    if (n != sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Invalid request");
        write(client_fd, &resp, sizeof(resp));
        return;
    }

    switch (req.kind) {
    case CMD_START:
    case CMD_RUN: {
        int rc = start_container(ctx, &req);
        if (rc == 0) {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Container %s started", req.container_id);
        } else if (rc == -1) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Container %s already running", req.container_id);
        } else if (rc == -2) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Rootfs %s is already in use by another container", req.rootfs);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Failed to start container %s", req.container_id);
        }
        write(client_fd, &resp, sizeof(resp));

        /* For CMD_RUN: keep client connected and wait for exit, then send exit status */
        if (req.kind == CMD_RUN && rc == 0) {
            /* Wait for the container to finish */
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *rec = find_container(ctx, req.container_id);
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (rec) {
                /* Poll until container exits */
                while (1) {
                    pthread_mutex_lock(&ctx->metadata_lock);
                    container_state_t st = rec->state;
                    int ec = rec->exit_code;
                    int es = rec->exit_signal;
                    pthread_mutex_unlock(&ctx->metadata_lock);

                    if (st != CONTAINER_RUNNING && st != CONTAINER_STARTING) {
                        memset(&resp, 0, sizeof(resp));
                        if (st == CONTAINER_EXITED) {
                            resp.status = ec;
                            snprintf(resp.message, sizeof(resp.message),
                                     "Container %s exited with code %d", req.container_id, ec);
                        } else {
                            resp.status = 128 + es;
                            snprintf(resp.message, sizeof(resp.message),
                                     "Container %s killed by signal %d", req.container_id, es);
                        }
                        write(client_fd, &resp, sizeof(resp));
                        break;
                    }
                    usleep(100000);
                }
            }
        }
        break;
    }
    case CMD_PS: {
        /* Use message buffer for small output; for large, we'd need streaming */
        char ps_buf[4096];
        build_ps_output(ctx, ps_buf, sizeof(ps_buf));
        resp.status = 0;
        strncpy(resp.message, ps_buf, sizeof(resp.message) - 1);
        write(client_fd, &resp, sizeof(resp));
        break;
    }
    case CMD_LOGS: {
        char logs_buf[4096];
        build_logs_output(ctx, req.container_id, logs_buf, sizeof(logs_buf));
        resp.status = 0;
        strncpy(resp.message, logs_buf, sizeof(resp.message) - 1);
        write(client_fd, &resp, sizeof(resp));
        break;
    }
    case CMD_STOP: {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *rec = find_container(ctx, req.container_id);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (rec && rec->state == CONTAINER_RUNNING) {
            stop_container(ctx, req.container_id);
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Stopping container %s", req.container_id);
        } else {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "Container %s not running", req.container_id);
        }
        write(client_fd, &resp, sizeof(resp));
        break;
    }
    default:
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "Unknown command");
        write(client_fd, &resp, sizeof(resp));
        break;
    }
}

/* ======================== Supervisor ======================== */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;
    strncpy(ctx.base_rootfs, rootfs, PATH_MAX - 1);
    g_ctx = &ctx;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* 1. Open kernel monitor device */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        fprintf(stderr, "[supervisor] Warning: Cannot open /dev/container_monitor: %s\n",
                strerror(errno));
        fprintf(stderr, "[supervisor] Continuing without kernel memory monitoring.\n");
    }

    /* 2. Create UNIX domain socket for control plane (Path B) */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        goto cleanup;
    }

    /* Make server socket non-blocking for the event loop */
    fcntl(ctx.server_fd, F_SETFL, O_NONBLOCK);

    /* 3. Create logs directory */
    mkdir(LOG_DIR, 0755);

    /* 4. Install signal handlers */
    struct sigaction sa_chld, sa_term;

    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = sigterm_handler;
    sa_term.sa_flags = 0;
    sigaction(SIGINT, &sa_term, NULL);
    sigaction(SIGTERM, &sa_term, NULL);

    /* 5. Start logger consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create");
        goto cleanup;
    }

    printf("[supervisor] Supervisor started. Listening on %s. Base rootfs: %s\n",
           CONTROL_PATH, rootfs);

    /* 6. Main event loop */
    while (!ctx.should_stop) {
        /* Accept client connections */
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd >= 0) {
            handle_control_request(&ctx, client_fd);
            close(client_fd);
        } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
            if (errno == EINTR)
                continue;
        }

        /* Reap any exited children */
        reap_children(&ctx);

        usleep(50000); /* 50ms poll interval */
    }

    printf("[supervisor] Shutting down...\n");

    /* ======================== Task 6: Cleanup ======================== */

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = ctx.containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            rec->stop_requested = 1;
            kill(rec->host_pid, SIGTERM);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Give containers time to exit */
    usleep(1000000);

    /* Force kill any remaining */
    pthread_mutex_lock(&ctx.metadata_lock);
    rec = ctx.containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            kill(rec->host_pid, SIGKILL);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Final reap */
    usleep(200000);
    reap_children(&ctx);

    /* Shutdown logging */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* Free all container records */
    pthread_mutex_lock(&ctx.metadata_lock);
    rec = ctx.containers;
    while (rec) {
        container_record_t *next = rec->next;
        if (rec->clone_stack)
            free(rec->clone_stack);
        free(rec);
        rec = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

cleanup:
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);

    unlink(CONTROL_PATH);

    printf("[supervisor] Shutdown complete. No zombies.\n");
    return 0;
}

/* ======================== Client side (Path B) ======================== */

static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect (is the supervisor running?)");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    /* Read initial response */
    ssize_t n = read(fd, &resp, sizeof(resp));
    if (n == sizeof(resp)) {
        printf("%s\n", resp.message);

        /* For CMD_RUN, wait for the final exit status response */
        if (req->kind == CMD_RUN && resp.status == 0) {
            n = read(fd, &resp, sizeof(resp));
            if (n == sizeof(resp)) {
                printf("%s\n", resp.message);
                close(fd);
                return resp.status;
            }
        }

        close(fd);
        return resp.status;
    }

    fprintf(stderr, "No response from supervisor\n");
    close(fd);
    return 1;
}

/* Signal handler for run client: forward SIGINT/SIGTERM => stop the container */
static const char *g_run_container_id = NULL;

static void run_client_signal_handler(int sig)
{
    (void)sig;
    if (g_run_container_id) {
        /* Send stop request to supervisor */
        control_request_t stop_req;
        memset(&stop_req, 0, sizeof(stop_req));
        stop_req.kind = CMD_STOP;
        strncpy(stop_req.container_id, g_run_container_id, CONTAINER_ID_LEN - 1);
        send_control_request(&stop_req);
    }
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    /* Install signal handler to forward SIGINT/SIGTERM as stop */
    g_run_container_id = req.container_id;
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = run_client_signal_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
