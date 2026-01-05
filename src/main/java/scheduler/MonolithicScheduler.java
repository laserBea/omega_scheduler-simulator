package scheduler;

import simulator.core.Job;
import simulator.core.ClaimDelta;
import simulator.core.CellState;

import java.util.*;

/**
 * Monolithic scheduler implementation.
 * Uses a simple FIFO queue and directly accesses the shared cell state.
 */
public class MonolithicScheduler extends BaseScheduler {
    
    public MonolithicScheduler(String name,
                              Map<String, Double> constantThinkTimes,
                              Map<String, Double> perTaskThinkTimes,
                              int numMachinesToBlackList) {
        super(name, constantThinkTimes, perTaskThinkTimes, numMachinesToBlackList);
        
        System.out.println(String.format("scheduler-id-info: %d, %s, %d, %s, %s",
            Thread.currentThread().getId(),
            name,
            hashCode(),
            constantThinkTimes.toString(),
            perTaskThinkTimes.toString()));
    }
    
    @Override
    public void addJob(Job job) {
        checkRegistered();
        job.setLastEnqueued(simulator.getCurrentTime());
        pendingQueue.offer(job);
        simulator.log("enqueued job " + job.getId());
        
        if (!scheduling) {
            scheduleNextJobAction();
        }
    }
    
    /**
     * Checks if there is a job in the queue and schedules it.
     */
    public void scheduleNextJobAction() {
        checkRegistered();
        
        if (!scheduling && !pendingQueue.isEmpty()) {
            scheduling = true;
            Job job = pendingQueue.poll();
            job.updateTimeInQueueStats(simulator.getCurrentTime());
            job.setLastSchedulingStartTime(simulator.getCurrentTime());
            double thinkTime = getThinkTime(job);
            
            simulator.log("getThinkTime returned " + thinkTime);
            
            // Schedule the job after think time
            final Job finalJob = job;
            simulator.afterDelay(thinkTime, () -> {
                simulator.log(String.format(
                    "Scheduler %s finished scheduling job %d. " +
                    "Attempting to schedule next job in scheduler's pendingQueue.",
                    name, finalJob.getId()));
                
                finalJob.setNumSchedulingAttempts(finalJob.getNumSchedulingAttempts() + 1);
                finalJob.setNumTaskSchedulingAttempts(
                    finalJob.getNumTaskSchedulingAttempts() + finalJob.getUnscheduledTasks());
                
                List<ClaimDelta> claimDeltas = scheduleJob(finalJob, simulator.getCellState());
                
                if (!claimDeltas.isEmpty()) {
                    simulator.getCellState().scheduleEndEvents(claimDeltas);
                    finalJob.setUnscheduledTasks(
                        finalJob.getUnscheduledTasks() - claimDeltas.size());
                    simulator.log(String.format(
                        "scheduled %d tasks of job %d's, %d remaining.",
                        claimDeltas.size(), finalJob.getId(), finalJob.getUnscheduledTasks()));
                    numSuccessfulTransactions++;
                    numSuccessfulTaskTransactions += claimDeltas.size();
                    recordUsefulTimeScheduling(finalJob, thinkTime,
                        finalJob.getNumSchedulingAttempts() == 1);
                } else {
                    simulator.log(String.format(
                        "No tasks scheduled for job %d (%f cpu %f mem) " +
                        "during this scheduling attempt, not recording any busy time. " +
                        "%d unscheduled tasks remaining.",
                        finalJob.getId(), finalJob.getCpusPerTask(), finalJob.getMemPerTask(),
                        finalJob.getUnscheduledTasks()));
                }
                
                String jobEventType = "";
                
                // If job isn't fully scheduled, put it back in queue
                if (finalJob.getUnscheduledTasks() > 0) {
                    simulator.log(String.format(
                        "Job %s didn't fully schedule, %d / %d tasks remain " +
                        "(shape: %f cpus, %f mem). Putting it back in the queue",
                        finalJob.getId(), finalJob.getUnscheduledTasks(), finalJob.getNumTasks(),
                        finalJob.getCpusPerTask(), finalJob.getMemPerTask()));
                    
                    // Give up on job if it hasn't scheduled in 100 tries or after 1000 tries
                    if ((finalJob.getNumSchedulingAttempts() > 100 &&
                         finalJob.getUnscheduledTasks() == finalJob.getNumTasks()) ||
                        finalJob.getNumSchedulingAttempts() > 1000) {
                        System.out.println(String.format(
                            "Abandoning job %d (%f cpu %f mem) with %d/%d " +
                            "remaining tasks, after %d scheduling attempts.",
                            finalJob.getId(), finalJob.getCpusPerTask(), finalJob.getMemPerTask(),
                            finalJob.getUnscheduledTasks(), finalJob.getNumTasks(),
                            finalJob.getNumSchedulingAttempts()));
                        numJobsTimedOutScheduling++;
                        jobEventType = "abandoned";
                    } else {
                        // Re-queue the job after a delay
                        simulator.afterDelay(1.0, () -> addJob(finalJob));
                    }
                } else {
                    jobEventType = "fully-scheduled";
                }
                
                scheduling = false;
                scheduleNextJobAction();
            });
            
            simulator.log("Scheduler named '" + name + "' started scheduling job " + job.getId());
        }
    }
}
