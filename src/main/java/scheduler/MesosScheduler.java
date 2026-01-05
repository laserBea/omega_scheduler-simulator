package scheduler;

import simulator.core.Job;
import simulator.core.ClaimDelta;
import simulator.core.CellState;
import simulator.MesosSimulator;

import java.util.*;

/**
 * Mesos scheduler implementation.
 * Receives resource offers from a MesosAllocator and schedules jobs onto offered resources.
 */
public class MesosScheduler extends BaseScheduler {
    private MesosSimulator mesosSimulator;
    private final Queue<Offer> offerQueue = new LinkedList<>();
    private final boolean schedulePartialJobs;
    
    public MesosScheduler(String name,
                         Map<String, Double> constantThinkTimes,
                         Map<String, Double> perTaskThinkTimes,
                         boolean schedulePartialJobs,
                         int numMachinesToBlackList) {
        super(name, constantThinkTimes, perTaskThinkTimes, numMachinesToBlackList);
        this.schedulePartialJobs = schedulePartialJobs;
        
        System.out.println(String.format("scheduler-id-info: %d, %s, %d, %s, %s",
            Thread.currentThread().getId(),
            name,
            hashCode(),
            constantThinkTimes.toString(),
            perTaskThinkTimes.toString()));
    }
    
    public void setMesosSimulator(MesosSimulator mesosSimulator) {
        this.mesosSimulator = mesosSimulator;
        this.simulator = mesosSimulator;
    }
    
    public MesosSimulator getMesosSimulator() {
        return mesosSimulator;
    }
    
    @Override
    protected void checkRegistered() {
        super.checkRegistered();
        if (mesosSimulator == null) {
            throw new IllegalStateException(
                "This scheduler has not been added to a MesosSimulator yet.");
        }
    }
    
    /**
     * Called by the allocator to send a resource offer to this scheduler.
     */
    public void resourceOffer(Offer offer) {
        offerQueue.offer(offer);
        handleNextResourceOffer();
    }
    
    /**
     * Handles the next resource offer in the queue.
     */
    public void handleNextResourceOffer() {
        if (!scheduling && !offerQueue.isEmpty()) {
            scheduling = true;
            Offer offer = offerQueue.poll();
            
            simulator.log("------ In " + name + ".resourceOffer(offer " + offer.getId() + ").");
            
            List<ClaimDelta> offerResponse = new ArrayList<>();
            final double[] aggThinkTime = {0.0};
            
            // Try to schedule jobs while resources are available
            while (offer.getCellState().getAvailableCpus() > 0.000001 &&
                   offer.getCellState().getAvailableMem() > 0.000001 &&
                   !pendingQueue.isEmpty()) {
                
                Job job = pendingQueue.poll();
                job.updateTimeInQueueStats(simulator.getCurrentTime());
                double jobThinkTime = getThinkTime(job);
                aggThinkTime[0] += jobThinkTime;
                job.setNumSchedulingAttempts(job.getNumSchedulingAttempts() + 1);
                job.setNumTaskSchedulingAttempts(
                    job.getNumTaskSchedulingAttempts() + job.getUnscheduledTasks());
                
                // Check if at least one task can fit
                if (offer.getCellState().getAvailableCpus() > job.getCpusPerTask() &&
                    offer.getCellState().getAvailableMem() > job.getMemPerTask()) {
                    
                    List<ClaimDelta> claimDeltas = scheduleJob(job, offer.getCellState());
                    
                    if (!claimDeltas.isEmpty()) {
                        numSuccessfulTransactions++;
                        numSuccessfulTaskTransactions += claimDeltas.size();
                        recordUsefulTimeScheduling(job, jobThinkTime,
                            job.getNumSchedulingAttempts() == 1);
                        mesosSimulator.log(String.format(
                            "Setting up job %d to accept at least part of offer %d. " +
                            "About to spend %f seconds scheduling it. " +
                            "Assigning %d tasks to it.",
                            job.getId(), offer.getId(), jobThinkTime, claimDeltas.size()));
                        offerResponse.addAll(claimDeltas);
                        job.setUnscheduledTasks(job.getUnscheduledTasks() - claimDeltas.size());
                    } else {
                        mesosSimulator.log(String.format(
                            "Rejecting all of offer %d for job %d, which requires tasks " +
                            "with %f cpu, %f mem. Not counting busy time for this sched attempt.",
                            offer.getId(), job.getId(), job.getCpusPerTask(), job.getMemPerTask()));
                        numNoResourcesFoundSchedulingAttempts++;
                    }
                } else {
                    mesosSimulator.log(String.format(
                        "Short-path rejecting all of offer %d for job %d because a single " +
                        "one of its tasks (%f cpu, %f mem) wouldn't fit into the sum of " +
                        "the offer's private cell state's remaining resources (%f cpu, %f mem).",
                        offer.getId(), job.getId(), job.getCpusPerTask(), job.getMemPerTask(),
                        offer.getCellState().getAvailableCpus(), 
                        offer.getCellState().getAvailableMem()));
                }
                
                String jobEventType = "";
                
                // If job is only partially scheduled, put it back in queue
                if (job.getUnscheduledTasks() > 0) {
                    mesosSimulator.log(String.format(
                        "Job %d is [still] only partially scheduled, (%d out of %d its " +
                        "tasks remain unscheduled) so putting it back in the queue.",
                        job.getId(), job.getUnscheduledTasks(), job.getNumTasks()));
                    
                    // Give up on job if it hasn't scheduled in 100 tries or after 1000 tries
                    if ((job.getNumSchedulingAttempts() > 100 &&
                         job.getUnscheduledTasks() == job.getNumTasks()) ||
                        job.getNumSchedulingAttempts() > 1000) {
                        System.out.println(String.format(
                            "Abandoning job %d (%f cpu %f mem) with %d/%d " +
                            "remaining tasks, after %d scheduling attempts.",
                            job.getId(), job.getCpusPerTask(), job.getMemPerTask(),
                            job.getUnscheduledTasks(), job.getNumTasks(),
                            job.getNumSchedulingAttempts()));
                        numJobsTimedOutScheduling++;
                        jobEventType = "abandoned";
                    } else {
                        simulator.afterDelay(1.0, () -> addJob(job));
                    }
                    job.setLastEnqueued(simulator.getCurrentTime());
                } else {
                    jobEventType = "fully-scheduled";
                }
            }
            
            if (pendingQueue.isEmpty()) {
                mesosSimulator.log("After scheduling, " + name + "'s pending queue is " +
                                  "empty, canceling outstanding resource request.");
                mesosSimulator.getAllocator().cancelOfferRequest(this);
            } else {
                mesosSimulator.log(String.format(
                    "%s's pending queue still has %d jobs in it, but for some reason, " +
                    "they didn't fit into this offer, so it will patiently wait for more " +
                    "resource offers.", name, pendingQueue.size()));
            }
            
            // Send response to offer after aggregate think time
            final Offer finalOffer = offer;
            final List<ClaimDelta> finalResponse = offerResponse;
            final double finalAggThinkTime = aggThinkTime[0];
            mesosSimulator.afterDelay(finalAggThinkTime, () -> {
                mesosSimulator.log(String.format(
                    "Waited %f seconds of aggThinkTime, now responding to offer %d " +
                    "with %d responses after.", finalAggThinkTime, finalOffer.getId(),
                    finalResponse.size()));
                mesosSimulator.getAllocator().respondToOffer(finalOffer, finalResponse);
            });
            
            scheduling = false;
            handleNextResourceOffer();
        }
    }
    
    @Override
    public void addJob(Job job) {
        checkRegistered();
        
        simulator.log("========================================================");
        simulator.log(String.format(
            "addJOB: CellState total usage: %fcpus (%.1f%s), %fmem (%.1f%s).",
            simulator.getCellState().getTotalOccupiedCpus(),
            simulator.getCellState().getTotalOccupiedCpus() / 
                simulator.getCellState().getTotalCpus() * 100.0,
            "%",
            simulator.getCellState().getTotalOccupiedMem(),
            simulator.getCellState().getTotalOccupiedMem() / 
                simulator.getCellState().getTotalMem() * 100.0,
            "%"));
        
        job.setLastEnqueued(simulator.getCurrentTime());
        pendingQueue.offer(job);
        simulator.log("Enqueued job " + job.getId() + " of workload type " + 
                     job.getWorkloadName() + ".");
        mesosSimulator.getAllocator().requestOffer(this);
    }
    
    /**
     * Schedule all available resources in the cell state.
     * Used by the allocator to pessimistically lock resources.
     */
    public List<ClaimDelta> scheduleAllAvailable(CellState cellState, boolean locked) {
        List<ClaimDelta> claimDeltas = new ArrayList<>();
        
        for (int mID = 0; mID < cellState.getNumMachines(); mID++) {
            double cpusAvail = cellState.availableCpusPerMachine(mID);
            double memAvail = cellState.availableMemPerMachine(mID);
            
            if (cpusAvail > 0.0 || memAvail > 0.0) {
                ClaimDelta claimDelta = new ClaimDelta(
                    this,
                    mID,
                    cellState.getMachineSeqNum(mID),
                    -1.0,
                    cpusAvail,
                    memAvail
                );
                claimDelta.apply(cellState, locked);
                claimDeltas.add(claimDelta);
            }
        }
        
        return claimDeltas;
    }
}
