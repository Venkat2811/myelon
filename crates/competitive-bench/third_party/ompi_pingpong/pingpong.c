#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <time.h>

typedef struct {
    const char* mode; // unused (always 2 ranks)
    size_t message_size;
    uint64_t warmup;
    uint64_t num_messages;
    int json;
    uint64_t target_rate; // msgs/sec; 0 = max throughput
    uint64_t batch_size;
} Args;

static void usage() {
    fprintf(stderr, "Usage: mpirun -n 2 ./pingpong --message-size N [--warmup W] [--num-messages N] [--json]\n");
}

static Args parse_args(int argc, char** argv) {
    Args a = { .mode = "mpi", .message_size = 64, .warmup = 10000, .num_messages = 100000, .json = 0, .target_rate = 0, .batch_size = 1 };
    for (int i = 1; i < argc; ++i) {
        const char* arg = argv[i];
        if (strcmp(arg, "--message-size") == 0 || strcmp(arg, "-s") == 0) {
            const char* next = (i + 1 < argc ? argv[++i] : NULL);
            if (!next) { usage(); exit(1);} a.message_size = (size_t)strtoull(next, NULL, 10);
        } else if (strcmp(arg, "--warmup") == 0) {
            const char* next = (i + 1 < argc ? argv[++i] : NULL);
            if (!next) { usage(); exit(1);} a.warmup = (uint64_t)strtoull(next, NULL, 10);
        } else if (strcmp(arg, "--num-messages") == 0 || strcmp(arg, "-n") == 0) {
            const char* next = (i + 1 < argc ? argv[++i] : NULL);
            if (!next) { usage(); exit(1);} a.num_messages = (uint64_t)strtoull(next, NULL, 10);
        } else if (strcmp(arg, "--json") == 0) {
            a.json = 1;
        } else if (strcmp(arg, "--target-rate") == 0) {
            const char* next = (i + 1 < argc ? argv[++i] : NULL);
            if (!next) { usage(); exit(1);} a.target_rate = (uint64_t)strtoull(next, NULL, 10);
        } else if (strcmp(arg, "--batch-size") == 0) {
            const char* next = (i + 1 < argc ? argv[++i] : NULL);
            if (!next) { usage(); exit(1);} a.batch_size = (uint64_t)strtoull(next, NULL, 10);
        } else {
            usage(); exit(1);
        }
    }
    return a;
}

static int cmp_u64(const void* a, const void* b) {
    uint64_t ua = *(const uint64_t*)a;
    uint64_t ub = *(const uint64_t*)b;
    return (ua > ub) - (ua < ub);
}

static inline uint64_t monotonic_now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (size != 2) {
        if (rank == 0) fprintf(stderr, "This benchmark requires exactly 2 ranks\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    Args a = parse_args(argc, argv);
    char* buf = (char*)malloc(a.message_size);
    memset(buf, 0xCD, a.message_size);

    int peer = (rank == 0) ? 1 : 0;

    // Warmup
    for (uint64_t i = 0; i < a.warmup; ++i) {
        if (rank == 0) {
            MPI_Send(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD);
            MPI_Recv(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else {
            MPI_Recv(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Send(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD);
        }
    }

    // Print timer resolution (rank 0)
    if (rank == 0) {
        double tick = MPI_Wtick();
        fprintf(stderr, "[OMPI] MPI_Wtick=%.9f sec\n", tick);
    }

    // Benchmark
    uint64_t* samples = NULL;
    uint64_t* samples_co = NULL;
    if (rank == 0) samples = (uint64_t*)malloc(sizeof(uint64_t) * a.num_messages);
    if (rank == 0) samples_co = (uint64_t*)malloc(sizeof(uint64_t) * a.num_messages);

    MPI_Barrier(MPI_COMM_WORLD);
    double bench_start = MPI_Wtime();

    if (a.target_rate > 0) {
        // Use absolute CLOCK_MONOTONIC pacing to avoid drift
        const long interval_ns = (long)(1e9 / (double)a.target_rate);
        struct timespec base_ts; clock_gettime(CLOCK_MONOTONIC, &base_ts);
        for (uint64_t i = 0; i < a.num_messages; ++i) {
            if (rank == 0) {
                // intended = base + i*interval
                struct timespec intended_ts = base_ts;
                long add = interval_ns * (long)i;
                intended_ts.tv_sec  += add / 1000000000L;
                intended_ts.tv_nsec += add % 1000000000L;
                if (intended_ts.tv_nsec >= 1000000000L) { intended_ts.tv_sec++; intended_ts.tv_nsec -= 1000000000L; }
                // Pace to intended (absolute)
                clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &intended_ts, NULL);
                uint64_t intended_ns = (uint64_t)intended_ts.tv_sec * 1000000000ull + (uint64_t)intended_ts.tv_nsec;
                double t0 = MPI_Wtime();
                MPI_Send(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD);
                MPI_Recv(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                double t1 = MPI_Wtime();
                uint64_t recv_ns = monotonic_now_ns();
                samples[i] = (uint64_t)((t1 - t0) * 1e9);
                samples_co[i] = recv_ns >= intended_ns ? (recv_ns - intended_ns) : 0;
            } else {
                MPI_Recv(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Send(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD);
            }
        }
    } else {
        for (uint64_t i = 0; i < a.num_messages; ++i) {
            if (rank == 0) {
                double t0 = MPI_Wtime();
                MPI_Send(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD);
                MPI_Recv(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                double t1 = MPI_Wtime();
                uint64_t dt_ns = (uint64_t)((t1 - t0) * 1e9);
                samples[i] = dt_ns;
            } else {
                MPI_Recv(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Send(buf, (int)a.message_size, MPI_CHAR, peer, 0, MPI_COMM_WORLD);
            }
        }
    }

    double bench_dur = MPI_Wtime() - bench_start;

    if (rank == 0) {
        // Percentiles
        qsort(samples, (size_t)a.num_messages, sizeof(uint64_t), cmp_u64);
        uint64_t p1 = samples[(size_t)(0.01 * (a.num_messages - 1))];
        uint64_t p10 = samples[(size_t)(0.10 * (a.num_messages - 1))];
        uint64_t p25 = samples[(size_t)(0.25 * (a.num_messages - 1))];
        uint64_t p50 = samples[(size_t)(0.50 * (a.num_messages - 1))];
        uint64_t p90 = samples[(size_t)(0.90 * (a.num_messages - 1))];
        uint64_t p95 = samples[(size_t)(0.95 * (a.num_messages - 1))];
        uint64_t p99 = samples[(size_t)(0.99 * (a.num_messages - 1))];
        uint64_t minv = samples[0];
        uint64_t maxv = samples[a.num_messages - 1];
        long double sum = 0; for (uint64_t i = 0; i < a.num_messages; ++i) sum += samples[i];
        double mean = (double)(sum / a.num_messages);
        double throughput = (double)a.num_messages / bench_dur;

        if (a.json) {
            printf("{\n");
            printf("  \"config\": {\n");
            printf("    \"message_size\": %zu,\n", a.message_size);
            printf("    \"num_messages\": %" PRIu64 ",\n", a.num_messages);
            printf("    \"warmup_messages\": %" PRIu64 ",\n", a.warmup);
            printf("    \"buffer_size\": 0,\n");
            printf("    \"wait_strategy\": \"mpi\"\n");
            printf("  },\n");
            printf("  \"throughput\": %.6f,\n", throughput);
            printf("  \"messages_processed\": %" PRIu64 ",\n", a.num_messages);
            printf("  \"duration_secs\": %.6f,\n", bench_dur);
            printf("  \"latency_stats\": {\n");
            printf("    \"count\": %" PRIu64 ",\n", a.num_messages);
            printf("    \"min\": %" PRIu64 ",\n", minv);
            printf("    \"max\": %" PRIu64 ",\n", maxv);
            printf("    \"mean\": %.6f,\n", mean);
            printf("    \"stdev\": 0.0,\n");
            printf("    \"p1\": %" PRIu64 ",\n", p1);
            printf("    \"p10\": %" PRIu64 ",\n", p10);
            printf("    \"p25\": %" PRIu64 ",\n", p25);
            printf("    \"p50\": %" PRIu64 ",\n", p50);
            printf("    \"p90\": %" PRIu64 ",\n", p90);
            printf("    \"p95\": %" PRIu64 ",\n", p95);
            printf("    \"p99\": %" PRIu64 ",\n", p99);
            // derive extended percentiles
            uint64_t p999 = samples[(size_t)(0.999 * (a.num_messages - 1))];
            uint64_t p9999 = samples[(size_t)(0.9999 * (a.num_messages - 1))];
            uint64_t p99999 = samples[(size_t)(0.99999 * (a.num_messages - 1))];
            uint64_t p999999 = samples[(size_t)(0.999999 * (a.num_messages - 1))];
            printf("    \"p999\": %" PRIu64 ",\n", p999);
            printf("    \"p9999\": %" PRIu64 ",\n", p9999);
            printf("    \"p99999\": %" PRIu64 ",\n", p99999);
            printf("    \"p999999\": %" PRIu64 "\n", p999999);
            printf("  },\n");
            if (a.target_rate > 0) {
                // CO stats
                // Sort a copy of samples_co
                qsort(samples_co, (size_t)a.num_messages, sizeof(uint64_t), cmp_u64);
                uint64_t p1c = samples_co[(size_t)(0.01 * (a.num_messages - 1))];
                uint64_t p10c = samples_co[(size_t)(0.10 * (a.num_messages - 1))];
                uint64_t p25c = samples_co[(size_t)(0.25 * (a.num_messages - 1))];
                uint64_t p50c = samples_co[(size_t)(0.50 * (a.num_messages - 1))];
                uint64_t p90c = samples_co[(size_t)(0.90 * (a.num_messages - 1))];
                uint64_t p95c = samples_co[(size_t)(0.95 * (a.num_messages - 1))];
                uint64_t p99c = samples_co[(size_t)(0.99 * (a.num_messages - 1))];
                uint64_t minc = samples_co[0];
                uint64_t maxc = samples_co[a.num_messages - 1];
                long double sumc = 0; for (uint64_t i = 0; i < a.num_messages; ++i) sumc += samples_co[i];
                double meanc = (double)(sumc / a.num_messages);
                printf("  \"measurement_mode\": \"fixed_rate\",\n");
                printf("  \"target_rate\": %" PRIu64 ",\n", a.target_rate);
                printf("  \"coordinated_omission_stats\": {\n");
                printf("    \"count\": %" PRIu64 ",\n", a.num_messages);
                printf("    \"min\": %" PRIu64 ",\n", minc);
                printf("    \"max\": %" PRIu64 ",\n", maxc);
                printf("    \"mean\": %.6f,\n", meanc);
                printf("    \"stdev\": 0.0,\n");
                printf("    \"p1\": %" PRIu64 ",\n", p1c);
                printf("    \"p10\": %" PRIu64 ",\n", p10c);
                printf("    \"p25\": %" PRIu64 ",\n", p25c);
                printf("    \"p50\": %" PRIu64 ",\n", p50c);
                printf("    \"p90\": %" PRIu64 ",\n", p90c);
                printf("    \"p95\": %" PRIu64 ",\n", p95c);
                printf("    \"p99\": %" PRIu64 ",\n", p99c);
                uint64_t p999c = samples_co[(size_t)(0.999 * (a.num_messages - 1))];
                uint64_t p9999c = samples_co[(size_t)(0.9999 * (a.num_messages - 1))];
                uint64_t p99999c = samples_co[(size_t)(0.99999 * (a.num_messages - 1))];
                uint64_t p999999c = samples_co[(size_t)(0.999999 * (a.num_messages - 1))];
                printf("    \"p999\": %" PRIu64 ",\n", p999c);
                printf("    \"p9999\": %" PRIu64 ",\n", p9999c);
                printf("    \"p99999\": %" PRIu64 ",\n", p99999c);
                printf("    \"p999999\": %" PRIu64 "\n", p999999c);
                printf("  },\n");
            } else {
                printf("  \"measurement_mode\": \"max_throughput\",\n");
            }
            printf("  \"timestamp\": \"\"\n");
            printf("}\n");
        } else {
            printf("MPI Ping-Pong\n");
            printf("Size: %zu bytes\n", a.message_size);
            printf("Throughput: %.2f msg/s\n", throughput);
            printf("P50: %" PRIu64 " ns, P90: %" PRIu64 " ns, P99: %" PRIu64 " ns\n", p50, p90, p99);
        }
    }

    free(buf);
    if (samples) free(samples);
    MPI_Finalize();
    return 0;
}
