#include "scheduler.h"

#include <algorithm>
#include <stdexcept>

using namespace std;

bool compareServerById(const ServerSpec &a, const ServerSpec &b) {
    return a.server_id < b.server_id;
}

bool compareJobByRelease(const Job &a, const Job &b) {
    if (a.release_time != b.release_time) return a.release_time < b.release_time;
    if (a.weight != b.weight) return a.weight > b.weight;  // 高权重优先
    if (a.duration != b.duration) return a.duration < b.duration;  // 短任务优先
    return a.job_id < b.job_id;
}

bool FinishEvent::operator>(const FinishEvent &other) const {
    if (finish_time != other.finish_time) return finish_time > other.finish_time;
    if (server_id != other.server_id) return server_id > other.server_id;
    return job_id > other.job_id;
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
    
    // 使用set替代queue，按优先级排序待处理任务
    PendingJobComparator comp(current_time);
    set<Job, PendingJobComparator> pending_jobs(comp);
    
    unordered_map<int, ScheduleRecord> records;
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> running_heap;

    while (static_cast<int>(records.size()) < static_cast<int>(jobs.size())) {
        releaseFinishedJobs(current_time, running_heap);

        // 新到达的任务加入待处理集合
        while (next_job_index < static_cast<int>(jobs.size()) &&
               jobs[next_job_index].release_time <= current_time) {
            pending_jobs.insert(jobs[next_job_index]);
            ++next_job_index;
        }

        tryStartPendingJobs(pending_jobs, current_time, records, running_heap);

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

void GreedyScheduler::tryStartPendingJobs(
    set<Job, PendingJobComparator> &pending_jobs,
    long long current_time,
    unordered_map<int, ScheduleRecord> &records,
    priority_queue<FinishEvent, vector<FinishEvent>, greater<FinishEvent>> &running_heap
) {
    // 由于current_time可能变化，set中的排序可能不再准确
    // 策略：每次尝试启动时，遍历set找到当前最优的可启动任务
    // 如果找到了，启动它，然后继续（因为资源状态变了，优先级也可能变）
    
    bool progress = true;
    while (progress && !pending_jobs.empty()) {
        progress = false;
        
        // 遍历set，按当前优先级顺序找到第一个能启动的任务
        for (auto it = pending_jobs.begin(); it != pending_jobs.end(); ++it) {
            const Job &job = *it;
            auto started = tryStartOneJob(job, current_time);
            
            if (started.has_value) {
                // 找到了可启动的高优先级任务
                records[job.job_id] = started.record;
                running_heap.push(
                    FinishEvent{
                        started.running_job.finish_time,
                        started.running_job.server_id,
                        started.running_job.job_id,
                        started.running_job,
                    }
                );
                
                // 从set中移除
                pending_jobs.erase(it);
                progress = true;
                break;  // 资源状态变了，重新从头遍历
            }
        }
    }
}

GreedyScheduler::StartResult GreedyScheduler::tryStartOneJob(const Job &job, long long current_time) {
    const vector<pair<int, int>> &entries = feasible_machines.at(job.job_id);
    
    int best_idx = -1;
    double best_score = -1e18;
    
    for (size_t idx = 0; idx < entries.size(); ++idx) {
        int machine_index = entries[idx].first;
        int gpu_used = entries[idx].second;
        
        if (!machines[machine_index].canStart(job, gpu_used)) {
            continue;
        }
        
        const MachineState &m = machines[machine_index];
        
        // ===== Best-Fit 评分系统 =====
        // 目标：最小化 E_memory = sum(p_i * (u_i * VG_s_i - v_i))
        // 即：最大化显存利用率，减少浪费
        
        // 1. 显存利用率（核心指标，越高越好）
        // 分配的显存 = gpu_used * VG_s, 实际使用 = v_i
        // 利用率 = v_i / (gpu_used * VG_s)
        double mem_utilization = static_cast<double>(job.gpu_memory) / 
                                  (gpu_used * m.spec.gpu_memory);
        
        // 2. GPU资源紧凑度（启动后剩余GPU越少越好，减少碎片）
        int remaining_after = m.getRemainingGpu() - gpu_used;
        double gpu_compactness = 1.0 - static_cast<double>(remaining_after) / m.spec.gpu_count;
        
        // 3. CPU利用率（越高越好，充分利用资源）
        double cpu_utilization = static_cast<double>(job.cpu_cores) / m.spec.cpu_cores;
        
        // 4. 内存利用率
        double memory_utilization = static_cast<double>(job.memory) / m.spec.memory;
        
        // 5. 服务器规模匹配度（优先使用小服务器，保留大服务器给大任务）
        double size_match = -static_cast<double>(abs(m.spec.gpu_count - gpu_used));
        
        // 6. 加权等待时间补偿（高权重任务如果等了很久，可以稍微放宽服务器选择）
        long long weighted_wait = static_cast<long long>(job.weight) * 
                                   (current_time - job.release_time);
        double wait_bonus = weighted_wait > 1000 ? 0.5 : 0.0;
        
        // 综合评分（显存效率最重要，其次是紧凑度）
        double score = mem_utilization * 1000.0      // 显存利用率权重最高
                     + gpu_compactness * 200.0        // GPU紧凑度
                     + cpu_utilization * 50.0       // CPU利用率
                     + memory_utilization * 50.0    // 内存利用率
                     + size_match * 5.0               // 规模匹配
                     + wait_bonus;                   // 等待补偿
        
        if (score > best_score) {
            best_score = score;
            best_idx = static_cast<int>(idx);
        }
    }
    
    if (best_idx == -1) {
        return StartResult{};  // 没有可行服务器
    }
    
    int machine_index = entries[best_idx].first;
    int gpu_used = entries[best_idx].second;
    pair<ScheduleRecord, RunningJob> result = machines[machine_index].startJob(job, current_time, gpu_used);
    return StartResult{true, result.first, result.second};
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