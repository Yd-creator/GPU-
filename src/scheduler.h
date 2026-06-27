#ifndef GPU_SCHEDULING_SCHEDULER_H
#define GPU_SCHEDULING_SCHEDULER_H

#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "machine_state.h"
#include "models.h"

struct FinishEvent {
    long long finish_time;
    int server_id;
    int job_id;
    RunningJob running_job;

    bool operator>(const FinishEvent &other) const;
};

// 待处理任务的优先级比较器
// 优先级规则（优先级越高，在set中越靠前）：
// 1. 加权等待时间越大越优先（等得越久的高权重任务越优先）
// 2. 权重越高越优先
// 3. 运行时长越短越优先（SPT - Shortest Processing Time）
// 4. 任务编号小优先（打破平局）
struct PendingJobComparator {
    long long current_time;

    explicit PendingJobComparator(long long time) : current_time(time) {}

    bool operator()(const Job &a, const Job &b) const {
        // 计算优先级分数（分数越高越优先）
        // 加权等待时间 = weight * (current_time - release_time)
        long long wait_a = static_cast<long long>(a.weight) * (current_time - a.release_time);
        long long wait_b = static_cast<long long>(b.weight) * (current_time - b.release_time);

        if (wait_a != wait_b) return wait_a > wait_b;
        if (a.weight != b.weight) return a.weight > b.weight;
        if (a.duration != b.duration) return a.duration < b.duration;
        return a.job_id < b.job_id;
    }
};

class GreedyScheduler {
public:
    GreedyScheduler(std::vector<ServerSpec> input_servers, std::vector<Job> input_jobs);

    std::vector<ScheduleRecord> schedule();

private:
    struct StartResult {
        bool has_value = false;
        ScheduleRecord record{};
        RunningJob running_job{};
    };

    void buildFeasibleMachines();
    void releaseFinishedJobs(
        long long current_time,
        std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    );
    
    // 使用set替代queue，支持按优先级排序
    void tryStartPendingJobs(
        std::set<Job, PendingJobComparator> &pending_jobs,
        long long current_time,
        std::unordered_map<int, ScheduleRecord> &records,
        std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    );
    
    // Best-Fit选择服务器
    StartResult tryStartOneJob(const Job &job, long long current_time);
    
    long long nextEventTime(
        long long current_time,
        int next_job_index,
        const std::priority_queue<FinishEvent, std::vector<FinishEvent>, std::greater<FinishEvent>> &running_heap
    ) const;

    std::vector<ServerSpec> servers;
    std::vector<Job> jobs;
    std::vector<MachineState> machines;
    std::unordered_map<int, int> machine_index_by_id;
    std::unordered_map<int, std::vector<std::pair<int, int>>> feasible_machines;
};

#endif