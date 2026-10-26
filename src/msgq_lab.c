/*
 * System Programming Lab 08 — POSIX Message Queues
 *
 * Student ID : 6813106
 * Name       : THANAPON THANADUNPREMDET
 */
#include "msgq_lab.h"

#include <errno.h>
#include <fcntl.h>
#include <mqueue.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

/* One queue name per task — prevents cross-contamination between runs */
#define Q_EXCHANGE "/msgq_lab_exchange"
#define Q_PRIO     "/msgq_lab_prio"
#define Q_FULL     "/msgq_lab_full"
#define Q_SIZE     "/msgq_lab_size"

/*
 * mq_msgsize used when opening every queue.
 * Must be >= sizeof(struct SensorReading) so a struct fits in one message.
 */
#define MSG_BUF_SIZE 128

/*
 * Wait for `pid` and report whether it exited normally with status 0.
 * Retries when interrupted by a signal.  Returns 0 on success, 1 otherwise.
 */
static int wait_child_ok(pid_t pid) {
    int status;
    while (waitpid(pid, &status, 0) == -1) {
        if (errno != EINTR) {
            perror("waitpid");
            return 1;
        }
    }
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 0 : 1;
}

/*
 * Task A — task_struct_exchange()
 *
 * Demonstrates: passing a struct between two processes via a POSIX message
 * queue.  Unlike a pipe, the queue persists until explicitly unlinked.
 *
 * Steps:
 *   1. mq_unlink(Q_EXCHANGE) — remove any leftover queue (ignore errors).
 *   2. Open the queue with mq_open():
 *        flags  = O_CREAT | O_RDWR
 *        mode   = 0660
 *        attr   = { .mq_maxmsg = 4, .mq_msgsize = MSG_BUF_SIZE }
 *   3. Fork one child process.
 *   4. Parent:
 *        a. Build SensorReading r = { .sensor_id = 7, .value = 42 }.
 *        b. mq_send(mqd, (char *)&r, sizeof(r), 0) — priority 0.
 *        c. mq_close(mqd).
 *        d. waitpid() for the child.
 *        e. Print exactly:  parent: sent reading
 *   5. Child:
 *        a. mq_receive(mqd, buf, MSG_BUF_SIZE, &prio) — blocks until message.
 *        b. Cast buf to SensorReading * and print exactly:
 *             child received: sensor id=7 value=42
 *        c. mq_close(mqd).
 *        d. mq_unlink(Q_EXCHANGE).
 *        e. exit(0).
 *
 * Note: mq_receive buffer size must be >= mq_msgsize (use MSG_BUF_SIZE).
 */
int task_struct_exchange(void) {
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg  = 4;
    attr.mq_msgsize = MSG_BUF_SIZE;

    mq_unlink(Q_EXCHANGE);                       /* leftover queue: ignore errors */

    mqd_t mqd = mq_open(Q_EXCHANGE, O_CREAT | O_RDWR, 0660, &attr);
    if (mqd == (mqd_t)-1) {
        perror("mq_open");
        return 1;
    }

    /* Empty stdio buffers so the child does not inherit (and re-print) them */
    fflush(stdout);

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        mq_close(mqd);
        mq_unlink(Q_EXCHANGE);
        return 1;
    }

    if (pid == 0) {
        /* ---- child: receive the struct ---- */
        _Alignas(struct SensorReading) char buf[MSG_BUF_SIZE];
        unsigned int prio = 0;

        ssize_t n = mq_receive(mqd, buf, MSG_BUF_SIZE, &prio);
        if (n != (ssize_t)sizeof(struct SensorReading)) {
            if (n < 0) perror("mq_receive");
            else       fprintf(stderr, "mq_receive: unexpected size %zd\n", n);
            mq_close(mqd);
            mq_unlink(Q_EXCHANGE);
            exit(1);
        }

        struct SensorReading *r = (struct SensorReading *)buf;
        printf("child received: sensor id=%d value=%d\n", r->sensor_id, r->value);
        fflush(stdout);

        mq_close(mqd);
        mq_unlink(Q_EXCHANGE);
        exit(0);
    }

    /* ---- parent: send the struct ---- */
    struct SensorReading r = { .sensor_id = 7, .value = 42 };
    int rc = 0;

    if (mq_send(mqd, (char *)&r, sizeof(r), 0) == -1) {
        perror("mq_send");
        rc = 1;
    }
    mq_close(mqd);

    if (wait_child_ok(pid) != 0) rc = 1;

    mq_unlink(Q_EXCHANGE);                       /* no-op if the child already did it */

    if (rc == 0) {
        printf("parent: sent reading\n");
        fflush(stdout);
    }
    return rc;
}

/*
 * Task B — task_priority_order()
 *
 * Demonstrates: POSIX message queues deliver messages highest-priority-first,
 * regardless of send order (key difference from pipes and System V queues with
 * type=0).
 *
 * Steps:
 *   1. mq_unlink(Q_PRIO) — clean slate.
 *   2. Open the queue O_CREAT | O_RDWR with mq_maxmsg=4, mq_msgsize=MSG_BUF_SIZE.
 *   3. Before forking, send TWO messages:
 *        a. SensorReading { .sensor_id = 1, .value = 10 } at priority 1  (first)
 *        b. SensorReading { .sensor_id = 2, .value = 99 } at priority 5  (second)
 *   4. mq_close(mqd), then fork().
 *   5. Child:
 *        a. Re-open the queue O_RDONLY (no O_CREAT).
 *        b. First mq_receive → highest priority wins → value=99.
 *           Print exactly:  child received hi-pri: value=99
 *        c. Second mq_receive → remaining message → value=10.
 *           Print exactly:  child received lo-pri: value=10
 *        d. mq_close, mq_unlink(Q_PRIO), exit(0).
 *   6. Parent: waitpid() for the child.
 *
 * Key insight: value=10 was enqueued first but is dequeued LAST because its
 * priority (1) is lower than priority (5).
 */
int task_priority_order(void) {
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg  = 4;
    attr.mq_msgsize = MSG_BUF_SIZE;

    mq_unlink(Q_PRIO);

    mqd_t mqd = mq_open(Q_PRIO, O_CREAT | O_RDWR, 0660, &attr);
    if (mqd == (mqd_t)-1) {
        perror("mq_open");
        return 1;
    }

    /* Send the low-priority message FIRST, the high-priority one SECOND */
    struct SensorReading lo = { .sensor_id = 1, .value = 10 };
    struct SensorReading hi = { .sensor_id = 2, .value = 99 };

    if (mq_send(mqd, (char *)&lo, sizeof(lo), 1) == -1 ||
        mq_send(mqd, (char *)&hi, sizeof(hi), 5) == -1) {
        perror("mq_send");
        mq_close(mqd);
        mq_unlink(Q_PRIO);
        return 1;
    }
    mq_close(mqd);

    fflush(stdout);

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        mq_unlink(Q_PRIO);
        return 1;
    }

    if (pid == 0) {
        /* ---- child: drain the queue, highest priority first ---- */
        _Alignas(struct SensorReading) char buf[MSG_BUF_SIZE];
        unsigned int prio = 0;

        mqd_t cq = mq_open(Q_PRIO, O_RDONLY);
        if (cq == (mqd_t)-1) {
            perror("mq_open (child)");
            exit(1);
        }

        ssize_t n = mq_receive(cq, buf, MSG_BUF_SIZE, &prio);
        if (n != (ssize_t)sizeof(struct SensorReading)) {
            if (n < 0) perror("mq_receive");
            mq_close(cq);
            mq_unlink(Q_PRIO);
            exit(1);
        }
        printf("child received hi-pri: value=%d\n",
               ((struct SensorReading *)buf)->value);

        n = mq_receive(cq, buf, MSG_BUF_SIZE, &prio);
        if (n != (ssize_t)sizeof(struct SensorReading)) {
            if (n < 0) perror("mq_receive");
            mq_close(cq);
            mq_unlink(Q_PRIO);
            exit(1);
        }
        printf("child received lo-pri: value=%d\n",
               ((struct SensorReading *)buf)->value);
        fflush(stdout);

        mq_close(cq);
        mq_unlink(Q_PRIO);
        exit(0);
    }

    /* ---- parent ---- */
    int rc = wait_child_ok(pid);
    mq_unlink(Q_PRIO);                           /* no-op if the child already did it */
    return rc;
}

/*
 * Task C — task_full_queue()
 *
 * Demonstrates: a bounded POSIX queue has a hard capacity of mq_maxmsg.
 * When the queue is full and O_NONBLOCK is set, mq_send() fails immediately
 * with errno == EAGAIN instead of blocking.
 *
 * Steps (no fork required):
 *   1. mq_unlink(Q_FULL) — clean slate.
 *   2. Open the queue with mq_maxmsg=2, mq_msgsize=MSG_BUF_SIZE.
 *      Include O_NONBLOCK in the flags.
 *   3. Send two messages (both succeed — queue is now full).
 *   4. Attempt a third mq_send().
 *      It must fail: errno == EAGAIN.
 *   5. If errno == EAGAIN, print exactly:  queue full: EAGAIN detected
 *   6. mq_close, mq_unlink(Q_FULL).
 */
int task_full_queue(void) {
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg  = 2;
    attr.mq_msgsize = MSG_BUF_SIZE;

    mq_unlink(Q_FULL);

    mqd_t mqd = mq_open(Q_FULL, O_CREAT | O_RDWR | O_NONBLOCK, 0660, &attr);
    if (mqd == (mqd_t)-1) {
        perror("mq_open");
        return 1;
    }

    struct SensorReading r = { .sensor_id = 3, .value = 1 };
    int rc = 1;

    /* Fill the queue: mq_maxmsg == 2, so two sends succeed */
    if (mq_send(mqd, (char *)&r, sizeof(r), 0) == -1 ||
        mq_send(mqd, (char *)&r, sizeof(r), 0) == -1) {
        perror("mq_send (fill)");
    } else if (mq_send(mqd, (char *)&r, sizeof(r), 0) == -1 && errno == EAGAIN) {
        /* Third send on a full O_NONBLOCK queue must fail with EAGAIN */
        printf("queue full: EAGAIN detected\n");
        fflush(stdout);
        rc = 0;
    } else {
        fprintf(stderr, "third mq_send did not fail with EAGAIN\n");
    }

    mq_close(mqd);
    mq_unlink(Q_FULL);
    return rc;
}

/*
 * Task D — task_size_limit()
 *
 * Demonstrates: mq_send() enforces mq_msgsize; any payload whose byte length
 * exceeds mq_msgsize causes the call to return -1 with errno == EMSGSIZE.
 *
 * Steps (no fork required):
 *   1. mq_unlink(Q_SIZE) — clean slate.
 *   2. Open the queue with mq_msgsize = sizeof(struct SensorReading).
 *      Use O_CREAT | O_WRONLY.
 *   3. Prepare a char buffer of size sizeof(struct SensorReading) + 1.
 *   4. Call mq_send() with that buffer — it must fail: errno == EMSGSIZE.
 *   5. If errno == EMSGSIZE, print exactly:  oversized send: EMSGSIZE detected
 *   6. mq_close, mq_unlink(Q_SIZE).
 */
int task_size_limit(void) {
    struct mq_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.mq_maxmsg  = 4;
    attr.mq_msgsize = sizeof(struct SensorReading);

    mq_unlink(Q_SIZE);

    mqd_t mqd = mq_open(Q_SIZE, O_CREAT | O_WRONLY, 0660, &attr);
    if (mqd == (mqd_t)-1) {
        perror("mq_open");
        return 1;
    }

    /* One byte larger than the queue's mq_msgsize */
    char big[sizeof(struct SensorReading) + 1];
    memset(big, 0, sizeof(big));

    int rc = 1;
    if (mq_send(mqd, big, sizeof(big), 0) == -1 && errno == EMSGSIZE) {
        printf("oversized send: EMSGSIZE detected\n");
        fflush(stdout);
        rc = 0;
    } else {
        fprintf(stderr, "oversized mq_send did not fail with EMSGSIZE\n");
    }

    mq_close(mqd);
    mq_unlink(Q_SIZE);
    return rc;
}

/* ------------------------------------------------------------------ */
/* main — do not modify                                                */
/* ------------------------------------------------------------------ */
int main(void) {
    if (task_struct_exchange() != 0) return EXIT_FAILURE;
    if (task_priority_order()  != 0) return EXIT_FAILURE;
    if (task_full_queue()      != 0) return EXIT_FAILURE;
    if (task_size_limit()      != 0) return EXIT_FAILURE;
    printf("all tasks done\n");
    return EXIT_SUCCESS;
}
