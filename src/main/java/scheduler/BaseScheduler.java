package scheduler;

import simulator.core.Job;
import simulator.core.ClaimDelta;
import simulator.core.CellState;
import simulator.ClusterSimulator;

import java.util.*;

/**
 * Base abstract class for schedulers that provides common functionality.
 */
public abstract class BaseScheduler implements IScheduler {
    protected final String name;
    protected final Map<String, Double> constantThinkTimes;
    protected final Map<String, Double> perTaskThinkTimes;
    protected final int numMachinesToBlackList;
    
    protected final Queue<Job> pendingQueue = new LinkedList<>();
    protected ClusterSimulator simulator;
    protected boolean scheduling = false;
    
    // Statistics counters
    protected int numSuccessfulTransactions = 0;
    protected int numFailedTransactions = 0;
    protected int numRetriedTransactions = 0;
    protected int numJobsTimedOutScheduling = 0;
    protected int numSuccessfulTaskTransactions = 0;
    protected int numFailedTaskTransactions = 0;
    protected int numNoResourcesFoundSchedulingAttempts = 0;
    protected int failedFindVictimAttempts = 0;
    
    protected double totalUsefulTimeScheduling = 0.0;
    protected double totalWastedTimeScheduling = 0.0;
    protected double firstAttemptUsefulTimeScheduling = 0.0;
    protected double firstAttemptWastedTimeScheduling = 0.0;
    
    protected final Map<String, Double> perWorkloadUsefulTimeScheduling = new HashMap<>();
    protected final Map<String, Double> perWorkloadWastedTimeScheduling = new HashMap<>();
    
    public BaseScheduler(String name,
                        Map<String, Double> constantThinkTimes,
                        Map<String, Double> perTaskThinkTimes,
                        int numMachinesToBlackList) {
        this.name = name;
        this.constantThinkTimes = new HashMap<>(constantThinkTimes);
        this.perTaskThinkTimes = new HashMap<>(perTaskThinkTimes);
        this.numMachinesToBlackList = numMachinesToBlackList;
    }

    // Public getters for metrics so external runners can report results
    public int getNumSuccessfulTransactions() { return numSuccessfulTransactions; }
    public int getNumFailedTransactions() { return numFailedTransactions; }
    public int getNumRetriedTransactions() { return numRetriedTransactions; }
    public int getNumJobsTimedOutScheduling() { return numJobsTimedOutScheduling; }
    public int getNumSuccessfulTaskTransactions() { return numSuccessfulTaskTransactions; }
    public int getNumFailedTaskTransactions() { return numFailedTaskTransactions; }
    public int getNumNoResourcesFoundSchedulingAttempts() { return numNoResourcesFoundSchedulingAttempts; }
    public int getFailedFindVictimAttempts() { return failedFindVictimAttempts; }

    public double getTotalUsefulTimeScheduling() { return totalUsefulTimeScheduling; }
    public double getTotalWastedTimeScheduling() { return totalWastedTimeScheduling; }
    public double getFirstAttemptUsefulTimeScheduling() { return firstAttemptUsefulTimeScheduling; }
    public double getFirstAttemptWastedTimeScheduling() { return firstAttemptWastedTimeScheduling; }

    public Map<String, Double> getPerWorkloadUsefulTimeScheduling() { return perWorkloadUsefulTimeScheduling; }
    public Map<String, Double> getPerWorkloadWastedTimeScheduling() { return perWorkloadWastedTimeScheduling; }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public long getJobQueueSize() {
        return pendingQueue.size();
    }
    
    @Override
    public boolean isScheduling() {
        return scheduling;
    }
    
    @Override
    public void setSimulator(ClusterSimulator simulator) {
        this.simulator = simulator;
    }
    
    public ClusterSimulator getSimulator() {
        return simulator;
    }
    
    protected void checkRegistered() {
        if (simulator == null) {
            throw new IllegalStateException(
                "This scheduler has not been added to a simulator yet.");
        }
    }
    
    @Override
    public double getThinkTime(Job job) {
        String workloadName = job.getWorkloadName();
        if (!constantThinkTimes.containsKey(workloadName)) {
            throw new IllegalArgumentException(
                "No constant think time defined for workload: " + workloadName);
        }
        if (!perTaskThinkTimes.containsKey(workloadName)) {
            throw new IllegalArgumentException(
                "No per-task think time defined for workload: " + workloadName);
        }
        
        double constantTime = constantThinkTimes.get(workloadName);
        double perTaskTime = perTaskThinkTimes.get(workloadName);
        return constantTime + perTaskTime * job.getUnscheduledTasks();
    }
    
    /**
     * Schedule a job using the randomized first-fit scheduling algorithm.
     * This is the core scheduling logic shared by all schedulers.
     */
    @Override
    public List<ClaimDelta> scheduleJob(Job job, CellState cellState) {
        checkRegistered();
        if (cellState == null) {
            throw new IllegalArgumentException("CellState cannot be null");
        }
        
        if (job.getCpusPerTask() > cellState.getCpusPerMachine() ||
            job.getMemPerTask() > cellState.getMemPerMachine()) {
            throw new IllegalArgumentException(
                String.format("Job requires %f CPUs and %f mem per task, " +
                            "but machines only have %f CPUs and %f mem.",
                            job.getCpusPerTask(), job.getMemPerTask(),
                            cellState.getCpusPerMachine(), cellState.getMemPerMachine()));
        }
        
        List<ClaimDelta> claimDeltas = new ArrayList<>();
        
        // Create candidate pool of machine IDs
        List<Integer> candidatePool = new ArrayList<>();
        for (int i = 0; i < cellState.getNumMachines(); i++) {
            candidatePool.add(i);
        }
        
        int numRemainingTasks = job.getUnscheduledTasks();
        int remainingCandidates = Math.max(0, 
            cellState.getNumMachines() - numMachinesToBlackList);
        
        Random random = new Random();
        
        while (numRemainingTasks > 0 && remainingCandidates > 0) {
            // Pick a random machine from the candidate pool
            int candidateIndex = random.nextInt(remainingCandidates);
            int currMachID = candidatePool.get(candidateIndex);
            
            // Check if a task fits on this machine
            if (cellState.availableCpusPerMachine(currMachID) >= job.getCpusPerTask() &&
                cellState.availableMemPerMachine(currMachID) >= job.getMemPerTask()) {
                
                // Create a claim delta for this task
                ClaimDelta claimDelta = new ClaimDelta(
                    this,
                    currMachID,
                    cellState.getMachineSeqNum(currMachID),
                    job.getTaskDuration(),
                    job.getCpusPerTask(),
                    job.getMemPerTask()
                );
                
                claimDelta.apply(cellState, false);
                claimDeltas.add(claimDelta);
                numRemainingTasks--;
            } else {
                failedFindVictimAttempts++;
                // Move the chosen candidate to the end
                Collections.swap(candidatePool, candidateIndex, remainingCandidates - 1);
                remainingCandidates--;
            }
        }
        
        return claimDeltas;
    }
    
    protected void recordUsefulTimeScheduling(Job job, double timeScheduling, 
                                              boolean isFirstSchedAttempt) {
        checkRegistered();
        totalUsefulTimeScheduling += timeScheduling;
        if (isFirstSchedAttempt) {
            firstAttemptUsefulTimeScheduling += timeScheduling;
        }
        job.setUsefulTimeScheduling(job.getUsefulTimeScheduling() + timeScheduling);
        
        String workloadName = job.getWorkloadName();
        perWorkloadUsefulTimeScheduling.put(workloadName,
            perWorkloadUsefulTimeScheduling.getOrDefault(workloadName, 0.0) + timeScheduling);
    }
    
    protected void recordWastedTimeScheduling(Job job, double timeScheduling,
                                             boolean isFirstSchedAttempt) {
        checkRegistered();
        totalWastedTimeScheduling += timeScheduling;
        if (isFirstSchedAttempt) {
            firstAttemptWastedTimeScheduling += timeScheduling;
        }
        job.setWastedTimeScheduling(job.getWastedTimeScheduling() + timeScheduling);
        
        String workloadName = job.getWorkloadName();
        perWorkloadWastedTimeScheduling.put(workloadName,
            perWorkloadWastedTimeScheduling.getOrDefault(workloadName, 0.0) + timeScheduling);
    }
}
