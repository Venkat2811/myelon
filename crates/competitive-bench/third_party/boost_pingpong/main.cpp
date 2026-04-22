#include <boost/interprocess/ipc/message_queue.hpp>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#include <thread>

using namespace boost::interprocess;
using namespace std::chrono;

struct Args {
    std::string mode = "client"; // or server
    std::string base = "boost_pp";
    size_t message_size = 64;
    uint64_t warmup = 10000;
    uint64_t num_messages = 100000;
    bool json = false;
    uint64_t target_rate = 0; // msgs/sec; 0 = max throughput
    size_t batch_size = 1;
    size_t queue_len = 1024; // default length per queue
};

static void print_usage() {
    std::cout << "Usage: boost_pingpong --mode [server|client] [--base NAME] [--message-size N]"
              << " [--warmup W] [--num-messages N] [--json]\n";
}

static Args parse_args(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) { std::cerr << "Missing value for " << arg << "\n"; std::exit(1);} 
            return argv[++i];
        };
        if (arg == "--mode") a.mode = next();
        else if (arg == "--base") a.base = next();
        else if (arg == "--message-size" || arg == "-s") a.message_size = std::stoul(next());
        else if (arg == "--warmup") a.warmup = std::stoull(next());
        else if (arg == "--num-messages" || arg == "-n") a.num_messages = std::stoull(next());
        else if (arg == "--json") a.json = true;
        else if (arg == "--target-rate") a.target_rate = std::stoull(next());
        else if (arg == "--batch-size") a.batch_size = std::stoul(next());
        else if (arg == "--queue-len") a.queue_len = std::stoul(next());
        else if (arg == "--help" || arg == "-h") { print_usage(); std::exit(0);} 
        else { std::cerr << "Unknown arg: " << arg << "\n"; print_usage(); std::exit(1);} 
    }
    return a;
}

static std::string qname_ping(const std::string& base){ return base + "_ping"; }
static std::string qname_pong(const std::string& base){ return base + "_pong"; }

static void run_server(const Args& a) {
    // Server: receive on ping, send on pong
    // Ensure clean state
    message_queue::remove(qname_ping(a.base).c_str());
    message_queue::remove(qname_pong(a.base).c_str());

    // Create queues (client will open)
    // Use conservative queue length to avoid resource exhaustion on large messages
    size_t ql = a.queue_len;
    if (ql < 2) ql = 2;
    message_queue ping(create_only, qname_ping(a.base).c_str(), ql, a.message_size);
    message_queue pong(create_only, qname_pong(a.base).c_str(), ql, a.message_size);

    std::vector<char> buf(a.message_size);
    size_t recvd = 0; unsigned int prio = 0;
    while (true) {
        ping.receive(buf.data(), buf.size(), recvd, prio);
        pong.send(buf.data(), recvd, 0);
    }
}

static void run_client(const Args& a) {
    // Open existing queues (server must have created them)
    // If server not running in separate terminal, user can launch server manually
    message_queue ping(open_or_create, qname_ping(a.base).c_str(), a.queue_len, a.message_size);
    message_queue pong(open_or_create, qname_pong(a.base).c_str(), a.queue_len, a.message_size);

    std::vector<char> buf(a.message_size);
    std::memset(buf.data(), 0xAB, buf.size());

    // Warmup
    size_t recvd = 0; unsigned int prio = 0;
    for (uint64_t i = 0; i < a.warmup; ++i) {
        ping.send(buf.data(), buf.size(), 0);
        pong.receive(buf.data(), buf.size(), recvd, prio);
    }

    // Benchmark
    std::vector<uint64_t> samples; samples.reserve(a.num_messages);
    std::vector<uint64_t> samples_co; samples_co.reserve(a.num_messages);

    auto bench_start = steady_clock::now();
    if (a.target_rate > 0) {
        // Fixed-rate with CO correction
        const uint64_t interval_ns = 1'000'000'000ull / a.target_rate;
        auto base = steady_clock::now();
        for (uint64_t i = 0; i < a.num_messages; ++i) {
            auto intended = base + nanoseconds(interval_ns * i);
            // Pace to intended (absolute)
            while (true) {
                auto now = steady_clock::now();
                if (now >= intended) break;
                auto wait = intended - now;
                if (wait > 1ms) std::this_thread::sleep_for(wait - 100us);
                else std::this_thread::yield();
            }
            auto t0 = steady_clock::now();
            ping.send(buf.data(), buf.size(), 0);
            pong.receive(buf.data(), buf.size(), recvd, prio);
            auto recv = steady_clock::now();
            uint64_t rtt = (uint64_t)duration_cast<nanoseconds>(recv - t0).count();
            uint64_t co = (uint64_t)duration_cast<nanoseconds>(recv - intended).count();
            samples.push_back(rtt);
            samples_co.push_back(co);
        }
    } else {
        // Max throughput
        for (uint64_t i = 0; i < a.num_messages; ++i) {
            auto t0 = steady_clock::now();
            ping.send(buf.data(), buf.size(), 0);
            pong.receive(buf.data(), buf.size(), recvd, prio);
            auto dt = duration_cast<nanoseconds>(steady_clock::now() - t0).count();
            samples.push_back(static_cast<uint64_t>(dt));
        }
    }
    auto bench_dur = duration_cast<duration<double>>(steady_clock::now() - bench_start).count();
    double throughput = static_cast<double>(a.num_messages) / bench_dur;

    // Compute percentiles (simple nth_element)
    auto pct = [&](double p) -> uint64_t {
        if (samples.empty()) return static_cast<uint64_t>(0);
        size_t idx = static_cast<size_t>((p/100.0) * (samples.size()-1));
        std::nth_element(samples.begin(), samples.begin()+idx, samples.end());
        return static_cast<uint64_t>(samples[idx]);
    };
        uint64_t p1 = pct(1.0), p10 = pct(10.0), p25 = pct(25.0), p50 = pct(50.0), p90 = pct(90.0), p95 = pct(95.0), p99 = pct(99.0);
    auto pct_cap = [&](double p) -> uint64_t {
        if (samples.empty()) return 0ULL;
        size_t idx = (size_t)((p/100.0) * (samples.size()-1));
        if (idx >= samples.size()) idx = samples.size()-1;
        std::nth_element(samples.begin(), samples.begin()+idx, samples.end());
        return static_cast<uint64_t>(samples[idx]);
    };
    uint64_t p999 = pct_cap(99.9), p9999 = pct_cap(99.99), p99999 = pct_cap(99.999), p999999 = pct_cap(99.9999);
    auto [min_it, max_it] = std::minmax_element(samples.begin(), samples.end());
    uint64_t minv = samples.empty()?0:*min_it;
    uint64_t maxv = samples.empty()?0:*max_it;
    long double mean = 0;
    for (auto v: samples) mean += v;
    mean = samples.empty()?0:(mean / samples.size());

    if (a.json) {
        // Match disruptor JSON shape
        std::cout << "{\n";
        std::cout << "  \"config\": {\n";
        std::cout << "    \"message_size\": " << a.message_size << ",\n";
        std::cout << "    \"num_messages\": " << a.num_messages << ",\n";
        std::cout << "    \"warmup_messages\": " << a.warmup << ",\n";
        std::cout << "    \"buffer_size\": 0,\n";
        std::cout << "    \"wait_strategy\": \"boost_msg_queue\"\n";
        std::cout << "  },\n";
        std::cout << "  \"throughput\": " << throughput << ",\n";
        std::cout << "  \"messages_processed\": " << a.num_messages << ",\n";
        std::cout << "  \"duration_secs\": " << bench_dur << ",\n";
        std::cout << "  \"latency_stats\": {\n";
        std::cout << "    \"count\": " << samples.size() << ",\n";
        std::cout << "    \"min\": " << minv << ",\n";
        std::cout << "    \"max\": " << maxv << ",\n";
        std::cout << "    \"mean\": " << static_cast<double>(mean) << ",\n";
        std::cout << "    \"stdev\": 0.0,\n";
        std::cout << "    \"p1\": " << p1 << ",\n";
        std::cout << "    \"p10\": " << p10 << ",\n";
        std::cout << "    \"p25\": " << p25 << ",\n";
        std::cout << "    \"p50\": " << p50 << ",\n";
        std::cout << "    \"p90\": " << p90 << ",\n";
        std::cout << "    \"p95\": " << p95 << ",\n";
        std::cout << "    \"p99\": " << p99 << ",\n";
        std::cout << "    \"p999\": " << p999 << ",\n";
        std::cout << "    \"p9999\": " << p9999 << ",\n";
        std::cout << "    \"p99999\": " << p99999 << ",\n";
        std::cout << "    \"p999999\": " << p999999 << "\n";
        std::cout << "  },\n";
        if (a.target_rate > 0) {
            // CO stats
            auto pct_co = [&](double p) -> uint64_t {
                if (samples_co.empty()) return 0ULL;
                size_t idx = static_cast<size_t>((p/100.0) * (samples_co.size()-1));
                std::nth_element(samples_co.begin(), samples_co.begin()+idx, samples_co.end());
                return static_cast<uint64_t>(samples_co[idx]);
            };
            uint64_t p1co = pct_co(1.0), p10co = pct_co(10.0), p25co = pct_co(25.0), p50co = pct_co(50.0), p90co = pct_co(90.0), p95co = pct_co(95.0), p99co = pct_co(99.0);
            auto pctc_cap = [&](double p) -> uint64_t {
                if (samples_co.empty()) return 0ULL;
                size_t idx = (size_t)((p/100.0) * (samples_co.size()-1));
                if (idx >= samples_co.size()) idx = samples_co.size()-1;
                std::nth_element(samples_co.begin(), samples_co.begin()+idx, samples_co.end());
                return static_cast<uint64_t>(samples_co[idx]);
            };
            uint64_t p999co = pctc_cap(99.9), p9999co = pctc_cap(99.99), p99999co = pctc_cap(99.999), p999999co = pctc_cap(99.9999);
            auto [minc_it, maxc_it] = std::minmax_element(samples_co.begin(), samples_co.end());
            uint64_t minc = samples_co.empty()?0:*minc_it;
            uint64_t maxc = samples_co.empty()?0:*maxc_it;
            long double meanco = 0; for (auto v: samples_co) meanco += v; meanco = samples_co.empty()?0:(meanco/samples_co.size());
            std::cout << "  \"measurement_mode\": \"fixed_rate\",\n";
            std::cout << "  \"target_rate\": " << a.target_rate << ",\n";
            std::cout << "  \"coordinated_omission_stats\": {\n";
            std::cout << "    \"count\": " << samples_co.size() << ",\n";
            std::cout << "    \"min\": " << minc << ",\n";
            std::cout << "    \"max\": " << maxc << ",\n";
            std::cout << "    \"mean\": " << static_cast<double>(meanco) << ",\n";
            std::cout << "    \"stdev\": 0.0,\n";
            std::cout << "    \"p1\": " << p1co << ",\n";
            std::cout << "    \"p10\": " << p10co << ",\n";
            std::cout << "    \"p25\": " << p25co << ",\n";
            std::cout << "    \"p50\": " << p50co << ",\n";
            std::cout << "    \"p90\": " << p90co << ",\n";
            std::cout << "    \"p95\": " << p95co << ",\n";
            std::cout << "    \"p99\": " << p99co << ",\n";
            std::cout << "    \"p999\": " << p999co << ",\n";
            std::cout << "    \"p9999\": " << p9999co << ",\n";
            std::cout << "    \"p99999\": " << p99999co << ",\n";
            std::cout << "    \"p999999\": " << p999999co << "\n";
            std::cout << "  },\n";
        } else {
            std::cout << "  \"measurement_mode\": \"max_throughput\",\n";
        }
        std::cout << "  \"timestamp\": \"\"\n";
        std::cout << "}\n";
    } else {
        std::cout << "Boost IPC Ping-Pong\n";
        std::cout << "Size: " << a.message_size << " bytes\n";
        std::cout << "Throughput: " << throughput << " msg/s\n";
        std::cout << "P50: " << p50 << " ns, P90: " << p90 << " ns, P99: " << p99 << " ns\n";
    }
}

int main(int argc, char** argv) {
    auto args = parse_args(argc, argv);
    try {
        if (args.mode == "server") {
            run_server(args);
        } else if (args.mode == "client") {
            // If server not started separately, user can run it first; otherwise open_or_create will create queues
            run_client(args);
        } else {
            print_usage();
            return 1;
        }
    } catch (const interprocess_exception& ex) {
        std::cerr << "boost::interprocess error: " << ex.what() << "\n";
        return 2;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << "\n";
        return 3;
    }
    return 0;
}
