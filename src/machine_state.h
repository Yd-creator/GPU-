#ifndef GPU_SCHEDULING_MACHINE_STATE_H
#define GPU_SCHEDULING_MACHINE_STATE_H

#include <utility>
#include <vector>
#include "models.h"

class MachineState {
public:
    explicit MachineState(ServerSpec server);

    long getGpuVram(int gpu_idx) const {
        return gpu_used_vram[gpu_idx];
    }

    int requiredGpuCount(const Job &job) const;
    bool canEverRun(const Job &job, int gpu_used) const;
    // 删除旧的单参数canStart，彻底消除冲突
    bool canAssignOnGpus(const std::vector<int> &gpu_ids, const Job &job) const;
    std::pair<ScheduleRecord, RunningJob> startJob(const std::vector<int> assign_gpus, long long current_time, const Job &job);
    void releaseJob(const RunningJob &running_job);

    int getRemainingGpu() const { return remaining_gpu; }
    int getRemainingCpu() const { return remaining_cpu; }
    long getMemoryLeft() const { return remaining_memory; }

    ServerSpec spec;

private:
    int remaining_gpu = 0;
    int remaining_cpu = 0;
    long remaining_memory = 0;
    std::vector<long> gpu_used_vram;
    std::vector<RunningJob> running_jobs;
};
#endif