// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
// Small native load generator for tools/pubsub/perfloop_pubsub.py.  Dragonfly's
// dfly_bench selects io_uring on Linux unconditionally, which cannot run on
// the older kernel used by this measurement environment.  This driver uses
// blocking TCP sockets, consumes every PUBLISH reply, and reports one latency
// distribution for a runtime-varied payload workload.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <barrier>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using Nanoseconds = std::chrono::nanoseconds;

struct Options {
  std::string host = "127.0.0.1";
  uint16_t port = 6379;
  unsigned connections = 1;
  unsigned duration_ms = 1000;
  size_t payload_bytes = 32;
  int expected_recipients = 1;
};

struct WorkerResult {
  std::vector<uint64_t> latencies_ns;
  std::string error;
};

[[noreturn]] void Die(std::string_view message) {
  std::cerr << "perfloop_pubsub_load: " << message << '\n';
  std::exit(2);
}

uint64_t ParseUnsigned(std::string_view text, std::string_view option) {
  uint64_t value = 0;
  const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (ec != std::errc{} || end != text.data() + text.size())
    Die(std::string("invalid value for ") + std::string(option));
  return value;
}

Options ParseOptions(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg{argv[i]};
    if (!arg.starts_with("--") || i + 1 == argc)
      Die("expected --option value pairs");

    const std::string_view value{argv[++i]};
    if (arg == "--host") {
      options.host = value;
    } else if (arg == "--port") {
      const auto parsed = ParseUnsigned(value, arg);
      if (parsed > std::numeric_limits<uint16_t>::max())
        Die("port is out of range");
      options.port = static_cast<uint16_t>(parsed);
    } else if (arg == "--connections") {
      options.connections = static_cast<unsigned>(ParseUnsigned(value, arg));
    } else if (arg == "--duration-ms") {
      options.duration_ms = static_cast<unsigned>(ParseUnsigned(value, arg));
    } else if (arg == "--payload-bytes") {
      options.payload_bytes = static_cast<size_t>(ParseUnsigned(value, arg));
    } else if (arg == "--expected-recipients") {
      options.expected_recipients = static_cast<int>(ParseUnsigned(value, arg));
    } else {
      Die(std::string("unknown option ") + std::string(arg));
    }
  }

  if (options.connections == 0 || options.duration_ms == 0 || options.payload_bytes == 0 ||
      options.expected_recipients <= 0) {
    Die("connections, duration, payload size, and expected recipients must be positive");
  }
  return options;
}

int Connect(const Options& options) {
  const int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd < 0)
    return -1;

  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

  timeval timeout{.tv_sec = 10, .tv_usec = 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(options.port);
  if (inet_pton(AF_INET, options.host.c_str(), &address.sin_addr) != 1 ||
      connect(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

bool SendAll(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    const ssize_t sent = send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent <= 0)
      return false;
    bytes.remove_prefix(static_cast<size_t>(sent));
  }
  return true;
}

class ReplyReader {
 public:
  explicit ReplyReader(int fd) : fd_(fd) {
  }

  bool ReadInteger(int* value) {
    while (true) {
      const size_t end = buffered_.find("\r\n");
      if (end != std::string::npos) {
        const std::string line = buffered_.substr(0, end);
        buffered_.erase(0, end + 2);
        if (line.size() < 2 || line.front() != ':')
          return false;
        const auto [parsed_end, ec] =
            std::from_chars(line.data() + 1, line.data() + line.size(), *value);
        return ec == std::errc{} && parsed_end == line.data() + line.size();
      }

      char scratch[512];
      const ssize_t received = recv(fd_, scratch, sizeof(scratch), 0);
      if (received <= 0)
        return false;
      buffered_.append(scratch, static_cast<size_t>(received));
    }
  }

 private:
  int fd_;
  std::string buffered_;
};

std::string EncodeBulk(std::string_view value) {
  return "$" + std::to_string(value.size()) + "\r\n" + std::string(value) + "\r\n";
}

std::string MakeRequest(uint64_t sequence, size_t payload_bytes) {
  std::string payload(payload_bytes, 'a');
  uint64_t state = sequence * 0x9e3779b97f4a7c15ULL +
                   static_cast<uint64_t>(Clock::now().time_since_epoch().count());
  constexpr char kDigits[] = "0123456789abcdef";
  for (size_t i = 0; i < payload.size(); ++i) {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    payload[i] = kDigits[state & 0xf];
  }

  std::string request{"*3\r\n"};
  request += EncodeBulk("PUBLISH");
  request += EncodeBulk("perfloop-pubsub");
  request += EncodeBulk(payload);
  return request;
}

void RunWorker(const Options& options, std::barrier<>& ready, std::atomic<bool>& begin,
               WorkerResult* result) {
  const int fd = Connect(options);
  if (fd < 0) {
    result->error = "failed to connect";
    ready.arrive_and_wait();
    return;
  }

  ReplyReader reader{fd};
  ready.arrive_and_wait();
  while (!begin.load(std::memory_order_acquire))
    std::this_thread::yield();

  const auto deadline = Clock::now() + std::chrono::milliseconds(options.duration_ms);
  uint64_t sequence = 0;
  while (Clock::now() < deadline) {
    std::string request = MakeRequest(sequence++, options.payload_bytes);
    const auto started = Clock::now();
    if (!SendAll(fd, request)) {
      result->error = "failed to send PUBLISH";
      break;
    }

    int recipients = 0;
    if (!reader.ReadInteger(&recipients)) {
      result->error = "failed to read PUBLISH reply";
      break;
    }
    if (recipients != options.expected_recipients) {
      result->error = "PUBLISH reply did not equal the live subscription count";
      break;
    }

    const auto elapsed = std::chrono::duration_cast<Nanoseconds>(Clock::now() - started).count();
    result->latencies_ns.push_back(static_cast<uint64_t>(elapsed));
  }

  close(fd);
}

uint64_t Percentile(const std::vector<uint64_t>& sorted, double percentile) {
  const double rank = percentile * static_cast<double>(sorted.size());
  size_t index = static_cast<size_t>(std::ceil(rank));
  index = index == 0 ? 0 : index - 1;
  return sorted[std::min(index, sorted.size() - 1)];
}

}  // namespace

int main(int argc, char** argv) {
  const Options options = ParseOptions(argc, argv);
  std::vector<WorkerResult> results(options.connections);
  std::barrier ready(static_cast<std::ptrdiff_t>(options.connections + 1));
  std::atomic<bool> begin{false};
  std::vector<std::thread> workers;
  workers.reserve(options.connections);

  for (unsigned i = 0; i < options.connections; ++i)
    workers.emplace_back(RunWorker, std::cref(options), std::ref(ready), std::ref(begin),
                         &results[i]);

  ready.arrive_and_wait();
  const auto wall_started = Clock::now();
  begin.store(true, std::memory_order_release);
  for (auto& worker : workers)
    worker.join();
  const auto wall_elapsed = Clock::now() - wall_started;

  std::vector<uint64_t> latencies;
  for (const auto& result : results) {
    if (!result.error.empty())
      Die(result.error);
    latencies.insert(latencies.end(), result.latencies_ns.begin(), result.latencies_ns.end());
  }
  if (latencies.empty())
    Die("no PUBLISH replies were recorded");

  std::sort(latencies.begin(), latencies.end());
  const double wall_seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(wall_elapsed).count();
  const auto to_ms = [](uint64_t nanoseconds) { return static_cast<double>(nanoseconds) / 1e6; };

  std::cout << std::fixed << std::setprecision(6) << "{\"count\":" << latencies.size()
            << ",\"p50_ms\":" << to_ms(Percentile(latencies, 0.50))
            << ",\"p99_ms\":" << to_ms(Percentile(latencies, 0.99))
            << ",\"ops_per_sec\":" << static_cast<double>(latencies.size()) / wall_seconds << "}\n";
  return 0;
}
