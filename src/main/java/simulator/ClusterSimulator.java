package simulator;

import scheduler.IScheduler;
import scheduler.BaseScheduler;
import simulator.core.CellState;
import simulator.core.Job;
import simulator.core.ClaimDelta;
import simulator.core.Workload;

import java.util.*;

/**
 * A simulator to compare different cluster scheduling architectures.
 * Manages schedulers, workloads, and the cell state.
 */
public class ClusterSimulator extends Simulator {
    protected final CellState cellState;
    protected final Map<String, IScheduler> schedulers;
    protected final Map<String, List<String>> workloadToSchedulerMap;
    protected final List<Workload> workloads;
    private final java.util.concurrent.atomic.AtomicInteger roundRobinCounter = new java.util.concurrent.atomic.AtomicInteger(0);
    
    public ClusterSimulator(CellState cellState,
                           Map<String, IScheduler> schedulers,
                           Map<String, List<String>> workloadToSchedulerMap,
                           List<Workload> workloads,
                           List<Workload> prefillWorkloads,
                           boolean logging) {
        super(logging);
        
        if (schedulers.isEmpty()) {
            throw new IllegalArgumentException(
                "At least one scheduler must be provided to simulator constructor.");
        }
        if (workloadToSchedulerMap.isEmpty()) {
            throw new IllegalArgumentException("No workload->scheduler map setup.");
        }
        
        // Validate workload-to-scheduler mappings
        for (List<String> schedulerNames : workloadToSchedulerMap.values()) {
            for (String schedulerName : schedulerNames) {
                if (!schedulers.containsKey(schedulerName)) {
                    throw new IllegalArgumentException(
                        "Workload-Scheduler map points to a scheduler, " +
                        schedulerName + ", that is not registered");
                }
            }
        }
        
        this.cellState = cellState;
        this.schedulers = schedulers;
        this.workloadToSchedulerMap = workloadToSchedulerMap;
        this.workloads = workloads;
        
        // Set up pointer to this simulator in the cell state
        cellState.setSimulator(this);
        
        // Set up pointer to this simulator in each scheduler
        for (IScheduler scheduler : schedulers.values()) {
            if (scheduler instanceof BaseScheduler) {
                ((BaseScheduler) scheduler).setSimulator(this);
            }
        }
        
        // Prefill jobs that exist at the beginning of the simulation
        prefillWorkloads(cellState, prefillWorkloads);
        
        // Set up workloads
        setupWorkloads(workloads);
    }
    
    public CellState getCellState() {
        return cellState;
    }
    
    public void log(String message) {
        super.log(message);
    }
    
    /**
     * Prefill the cell state with initial jobs.
     */
    private void prefillWorkloads(CellState cellState, List<Workload> prefillWorkloads) {
        System.out.println("Prefilling cell-state with " + prefillWorkloads.size() + " workloads.");
        
        for (Workload workload : prefillWorkloads) {
            System.out.println("Prefilling cell-state with " + workload.getNumJobs() + 
                             " jobs from workload " + workload.getName() + ".");
            
            for (Job job : workload.getJobs()) {
                if (job.getCpusPerTask() > cellState.getCpusPerMachine() ||
                    job.getMemPerTask() > cellState.getMemPerMachine()) {
                    System.out.println(String.format(
                        "IGNORING A JOB REQUIRING %f CPU & %f MEM PER TASK " +
                        "BECAUSE machines only have %f cpu / %f mem.",
                        job.getCpusPerTask(), job.getMemPerTask(),
                        cellState.getCpusPerMachine(), cellState.getMemPerMachine()));
                } else {
                    // Create a simple prefill scheduler
                    BaseScheduler prefillScheduler = new BaseScheduler("prefillScheduler",
                        new HashMap<>(), new HashMap<>(), 0) {
                        @Override
                        public void addJob(Job job) {
                            // No-op for prefill
                        }
                    };
                    prefillScheduler.setSimulator(this);
                    
                    // Schedule job directly without going through normal scheduling
                    List<ClaimDelta> claimDeltas = prefillScheduler.scheduleJob(job, cellState);
                    cellState.scheduleEndEvents(claimDeltas);
                    
                    log(String.format(
                        "After prefill, common cell state now has %.2f%% (%.2f) " +
                        "cpus and %.2f%% (%.2f) mem occupied.",
                        cellState.getTotalOccupiedCpus() / cellState.getTotalCpus() * 100.0,
                        cellState.getTotalOccupiedCpus(),
                        cellState.getTotalOccupiedMem() / cellState.getTotalMem() * 100.0,
                        cellState.getTotalOccupiedMem()));
                }
            }
        }
    }
    
    /**
     * Set up workloads by scheduling job arrivals.
     */
    private void setupWorkloads(List<Workload> workloads) {
        for (Workload workload : workloads) {
            int numSkipped = 0;
            int numLoaded = 0;
            
            for (Job job : workload.getJobs()) {
                IScheduler scheduler = getSchedulerForWorkloadName(job.getWorkloadName());
                
                if (scheduler == null) {
                    log("Warning, skipping a job from a workload type (" + 
                        job.getWorkloadName() + ") that has not been mapped to any " +
                        "registered schedulers. Please update a mapping for this scheduler.");
                    numSkipped++;
                } else {
                    // Validate job fits in cluster
                    if (job.getCpusPerTask() * job.getNumTasks() > 
                        cellState.getTotalCpus() + 0.000001 ||
                        job.getMemPerTask() * job.getNumTasks() > 
                        cellState.getTotalMem() + 0.000001) {
                        System.out.println(String.format(
                            "WARNING: The cell (%f cpus, %f mem) is not big enough " +
                            "to hold job id %d all at once which requires %f cpus " +
                            "and %f mem in total.",
                            cellState.getTotalCpus(), cellState.getTotalMem(),
                            job.getId(),
                            job.getCpusPerTask() * job.getNumTasks(),
                            job.getMemPerTask() * job.getNumTasks()));
                    }
                    
                    // Schedule the job to be submitted at its submission time
                    double delay = job.getSubmitted() - currentTime;
                    if (delay < 0) delay = 0;
                    afterDelay(delay, () -> scheduler.addJob(job));
                    numLoaded++;
                }
            }
            
            System.out.println("Loaded " + numLoaded + " jobs from workload " + 
                             workload.getName() + ", and skipped " + numSkipped + ".");
        }
    }
    
    /**
     * Get the scheduler for a workload name using round-robin if multiple schedulers.
     */
    private IScheduler getSchedulerForWorkloadName(String workloadName) {
        List<String> schedulerNames = workloadToSchedulerMap.get(workloadName);
        if (schedulerNames == null || schedulerNames.isEmpty()) {
            return null;
        }
        // Round-robin across schedulers (thread-safe implementation)
        if (schedulerNames.size() == 1) {
            return schedulers.get(schedulerNames.get(0));
        }
        // Use a round-robin counter for fair distribution
        int index = roundRobinCounter.getAndIncrement() % schedulerNames.size();
        String name = schedulerNames.get(index);
        return schedulers.get(name);
    }
    
    @Override
    public boolean run(Double runTime, Double wallClockTimeout) {
        if (currentTime != 0.0) {
            throw new IllegalStateException("currentTime must be 0 at simulator run time.");
        }
        
        // Validate schedulers have empty queues
        for (IScheduler scheduler : schedulers.values()) {
            if (scheduler.getJobQueueSize() > 0) {
                throw new IllegalStateException(
                    "Schedulers are not allowed to have jobs in their queues " +
                    "when we run the simulator.");
            }
        }
        
        return super.run(runTime, wallClockTimeout);
    }
}

