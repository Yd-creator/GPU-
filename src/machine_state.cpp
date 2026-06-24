#include "machine_state.h"
#include <algorithm>
#include <numeric>
using namespace std;

MachineState::MachineState(ServerSpec server) : spec(server) {
    remaining_gpu = spec.gpu_count;
    remaining_cpu = spec.cpu_cores;
    remaining_memory = spec.memory;
    // 初始化每张GPU占用显存为0
    gpu_used_vram.assign(spec.gpu_count, 0L);
}

int MachineState::requiredGpuCount(const Job &job) const {
    int gpu_for_memory = (job.gpu_memory + spec.gpu_memory - 1) / spec.gpu_memory;
    return max(job.min_gpu, gpu_for_memory);
}

bool MachineState::canEverRun(const Job &job, int gpu_used) const {
    return gpu_used <= spec.gpu_count &&
           job.cpu_cores <= spec.cpu_cores &&
           job.memory <= spec.memory;
}

// 判断选中的一批GPU能否容纳任务显存
bool MachineState::canAssignOnGpus(const vector<int> &gpu_ids, const Job &job) const {
    long need_per_gpu = job.gpu_memory / (long)gpu_ids.size();
    long mod = job.gpu_memory % (long)gpu_ids.size();
    for (int idx : gpu_ids) {
        long add = need_per_gpu;
        if (mod > 0) {
            add += 1;
            mod--;
        }
        if (gpu_used_vram[idx] + add > spec.gpu_memory) {
            return false;
        }
    }
    return true;
}

// bool MachineState::canStart(const Job &job, int gpu_used) const {
//     return gpu_used <= remaining_gpu &&
//            job.cpu_cores <= remaining_cpu &&
//            job.memory <= remaining_memory;
// }

// pair<ScheduleRecord, RunningJob> MachineState::startJob(const Job &job, long long current_time, int gpu_used) {
//     long long finish_time = current_time + job.duration;

//     remaining_gpu -= gpu_used;
//     remaining_cpu -= job.cpu_cores;
//     remaining_memory -= job.memory;

//     RunningJob running_job{
//         job.job_id,
//         spec.server_id,
//         finish_time,
//         gpu_used,
//         job.cpu_cores,
//         job.memory,
//     };
//     running_jobs.push_back(running_job);

//     ScheduleRecord record{
//         job.job_id,
//         spec.server_id,
//         current_time,
//         gpu_used,
//         finish_time,
//     };

//     return {record, running_job};
// }
// 分配任务到指定GPU列表
std::pair<ScheduleRecord, RunningJob> MachineState::startJob(std::vector<int> assign_gpus, long long current_time, const Job &job) {
    long long finish_time = current_time + job.duration;
    remaining_cpu -= job.cpu_cores;
    remaining_memory -= job.memory;
    remaining_gpu -= (int)assign_gpus.size();

    long total_need = job.gpu_memory;
    long per = total_need / assign_gpus.size();
    long rem = total_need % assign_gpus.size();
    for (int idx : assign_gpus) {
        long add = per;
        if (rem > 0) {
            add += 1;
            rem--;
        }
        gpu_used_vram[idx] += add;
    }

    RunningJob running_job{
        job.job_id,
        spec.server_id,
        finish_time,
        (int)assign_gpus.size(),
        job.cpu_cores,
        job.memory,
    };
    running_jobs.push_back(running_job);
    ScheduleRecord record{
        job.job_id,
        spec.server_id,
        current_time,
        (int)assign_gpus.size(),
        finish_time,
    };
    return std::make_pair(record, running_job);
}

// void MachineState::releaseJob(const RunningJob &running_job) {
//     remaining_gpu += running_job.gpu_used;
//     remaining_cpu += running_job.cpu_used;
//     remaining_memory += running_job.memory_used;

//     vector<RunningJob> remaining;
//     for (size_t i = 0; i < running_jobs.size(); ++i) {
//         if (running_jobs[i].job_id != running_job.job_id) {
//             remaining.push_back(running_jobs[i]);
//         }
//     }
//     running_jobs = remaining;
// }

void MachineState::releaseJob(const RunningJob &running_job) {
    // 遍历该任务，反向归还显存（简化：遍历所有运行任务匹配job_id，取回当时占用GPU数均分扣除）
    for (auto &rj : running_jobs) {
        if (rj.job_id == running_job.job_id) {
            long total_vram_used = rj.gpu_used * spec.gpu_memory;
            long per = total_vram_used / rj.gpu_used;
            long rem = total_vram_used % rj.gpu_used;
            int cnt = rj.gpu_used;
            // 找空闲GPU下标归还（简化逻辑：从0开始回收）
            int pos = 0;
            while (cnt > 0) {
                long sub = per;
                if (rem > 0) {
                    sub++; rem--;
                }
                gpu_used_vram[pos] -= sub;
                pos++;
                cnt--;
            }
            break;
        }
    }
    remaining_gpu += running_job.gpu_used;
    remaining_cpu += running_job.cpu_used;
    remaining_memory += running_job.memory_used;

    // 删除已结束任务
    running_jobs.erase(
        remove_if(running_jobs.begin(), running_jobs.end(),
                  [&](const RunningJob &rj) { return rj.job_id == running_job.job_id; }),
        running_jobs.end()
    );
}