#include "scheduler.h"

#include <algorithm>
#include <stdexcept>
#include <queue>
#include <numeric>

using namespace std;

// 全局调参常量（多维BF+动态优先级）
constexpr double ALPHA_DUR = 1.0;    // 运行时长权重(SJF)
constexpr double BETA_WAIT = 0.8;    // 等待饥饿补偿权重
constexpr double GAMMA_WEIGHT = 0.5; // 业务权重
constexpr double W_VRAM = 0.6;       // 显存浪费打分权重
constexpr double W_CPU = 0.2;        // CPU浪费打分权重
constexpr double W_MEM = 0.2;        // 内存浪费打分权重

bool compareServerById(const ServerSpec &a, const ServerSpec &b) {
    return a.server_id < b.server_id;
}

bool compareJobByRelease(const Job &a, const Job &b) {
    if (a.release_time != b.release_time) return a.release_time < b.release_time;
    if (a.duration != b.duration) return a.duration < b.duration;
    return a.job_id < b.job_id;
}

bool FinishEvent::operator>(const FinishEvent &other) const {
    if (finish_time != other.finish_time) return finish_time > other.finish_time;
    if (server_id != other.server_id) return server_id > other.server_id;
    return job_id > other.job_id;
}

// scheduler.cpp 全局位置添加比较器实现
bool PendingJobCmp::operator()(const PendingJob &a, const PendingJob &b) {
    double scoreA = ALPHA_DUR * a.job.duration - BETA_WAIT * a.wait_time - GAMMA_WEIGHT * a.job.weight;
    double scoreB = ALPHA_DUR * b.job.duration - BETA_WAIT * b.wait_time - GAMMA_WEIGHT * b.job.weight;
    return scoreA > scoreB;
}

GreedyScheduler::GreedyScheduler(vector<ServerSpec> input_servers, vector<Job> input_jobs)
    : servers(move(input_servers)), jobs(move(input_jobs)) {
    sort(servers.begin(), servers.end(), compareServerById);
    sort(jobs.begin(), jobs.end(), compareJobByRelease);

    for (const auto &server : servers) {
        machines.emplace_back(server);
    }
    for (int index = 0; index < static_cast<int>(machines.size()); ++index) {
        machine_index_by_id[machines[index].spec.server_id] = index;
    }

    buildFeasibleMachines();
}

vector<ScheduleRecord> GreedyScheduler::schedule() {
    if (jobs.empty()) {
        return {};
    }
    long long current_time = jobs.front().release_time;
    int next_job_index = 0;
    // 替换普通queue为动态优先级优先队列
    priority_queue<PendingJob, vector<PendingJob>, PendingJobCmp> pending_pq;
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;

    while (static_cast<int>(records.size()) < static_cast<int>(jobs.size())) {
        releaseFinishedJobs(current_time, running_heap);

        // 新到达任务加入优先队列，初始等待0
        while (next_job_index < static_cast<int>(jobs.size()) &&
               jobs[next_job_index].release_time <= current_time) {
            pending_pq.push({jobs[next_job_index], 0LL});
            ++next_job_index;
        }

        // 更新队列内所有任务的等待时长
        vector<PendingJob> temp;
        while (!pending_pq.empty()) {
            auto pj = pending_pq.top();
            pending_pq.pop();
            pj.wait_time = current_time - pj.job.release_time;
            temp.push_back(pj);
        }
        for (auto &pj : temp) pending_pq.push(pj);

        tryStartPendingJobs(pending_pq, current_time, records, running_heap);

        if (static_cast<int>(records.size()) == static_cast<int>(jobs.size())) {
            break;
        }
        current_time = nextEventTime(current_time, next_job_index, running_heap);
    }

    vector<ScheduleRecord> ordered_records;
    ordered_records.reserve(records.size());
    for (int job_id = 1; job_id <= static_cast<int>(jobs.size()); ++job_id) {
        ordered_records.push_back(records.at(job_id));
    }
    return ordered_records;
}

void GreedyScheduler::buildFeasibleMachines() {
    for (const auto &job : jobs) {
        vector<pair<int, int>> entries;

        for (int index = 0; index < static_cast<int>(machines.size()); ++index) {
            int gpu_used = machines[index].requiredGpuCount(job);
            if (machines[index].canEverRun(job, gpu_used)) {
                entries.push_back({index, gpu_used});
            }
        }

        if (entries.empty()) {
            throw runtime_error("A job cannot run on any server.");
        }

        feasible_machines[job.job_id] = entries;
    }
}

void GreedyScheduler::releaseFinishedJobs(
    long long current_time,
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) {
    while (!running_heap.empty() && running_heap.top().finish_time <= current_time) {
        FinishEvent event = running_heap.top();
        running_heap.pop();

        int machine_index = machine_index_by_id.at(event.server_id);
        machines[machine_index].releaseJob(event.running_job);
    }
}

// 参数修改：接收动态优先级优先队列
void GreedyScheduler::tryStartPendingJobs(
    priority_queue<PendingJob, vector<PendingJob>, PendingJobCmp> &pending_pq,
    long long current_time,
    unordered_map<int, ScheduleRecord> &records,
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) {
    vector<PendingJob> wait_back;
    while (!pending_pq.empty()) {
        PendingJob pj = pending_pq.top();
        pending_pq.pop();
        Job job = pj.job;

        auto started = tryStartOneJob(job, current_time);
        if (!started.has_value) {
            wait_back.push_back(pj);
            continue;
        }
        // 分配成功，记录调度结果
        records[job.job_id] = started.record;
        running_heap.push(
            FinishEvent{
                started.running_job.finish_time,
                started.running_job.server_id,
                started.running_job.job_id,
                started.running_job,
            }
        );
    }
    // 暂时无法执行的任务放回优先队列
    for (auto &x : wait_back) pending_pq.push(x);
}

核心改造：多维BestFit 替换原First-Fit
GreedyScheduler::StartResult GreedyScheduler::tryStartOneJob(const Job &job, long long current_time) {
    const vector<pair<int, int>> &entries = feasible_machines.at(job.job_id);
    vector<pair<int, double>> machine_score; // <机器索引, 浪费分>

    // 第一步：筛选当前空闲可启动的机器，计算多维浪费分数
    for (auto &entry : entries) {
        int midx = entry.first;
        int gpu_need = entry.second;
        auto &m = machines[midx];
        if (!m.canStart(job, gpu_need)) continue;

        // 分配后剩余资源 = 总空闲 - 任务占用
        long idle_vram = (long)m.spec.gpu_memory * (m.getRemainingGpu() - gpu_need);
        int idle_cpu = m.getRemainingCpu() - job.cpu_cores;
        long idle_mem = m.getMemoryLeft() - job.memory;

        // 多维资源浪费打分
        double waste = W_VRAM * idle_vram + W_CPU * idle_cpu + W_MEM * idle_mem;
        machine_score.emplace_back(midx, waste);
    }

    if (machine_score.empty()) return StartResult{};

    // 第二步：按浪费分升序排序（BestFit：余量最小优先）
    sort(machine_score.begin(), machine_score.end(),
        [](const pair<int, double> &a, const pair<int, double> &b) {
            return a.second < b.second;
        });

    // 选浪费最小第一台分配
    int best_midx = machine_score[0].first;
    int best_gpu = 0;
    for (auto &e : entries) {
        if (e.first == best_midx) {
            best_gpu = e.second;
            break;
        }
    }
    auto res = machines[best_midx].startJob(job, current_time, best_gpu);
    return StartResult{true, res.first, res.second};
}

long long GreedyScheduler::nextEventTime(
    long long current_time,
    int next_job_index,
    const priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) const {
    vector<long long> candidates;

    if (next_job_index < static_cast<int>(jobs.size())) {
        candidates.push_back(jobs[next_job_index].release_time);
    }
    if (!running_heap.empty()) {
        candidates.push_back(running_heap.top().finish_time);
    }

    long long next_time = -1;
    for (long long candidate : candidates) {
        if (candidate <= current_time) {
            continue;
        }
        if (next_time == -1 || candidate < next_time) {
            next_time = candidate;
        }
    }

    if (next_time == -1) {
        throw runtime_error("No future event exists.");
    }

    return next_time;
}

