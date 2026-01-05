package experiments;

import scheduler.MonolithicScheduler;
import scheduler.MesosAllocator;
import scheduler.MesosScheduler;
import scheduler.OmegaScheduler;
import simulator.ClusterSimulator;
import simulator.MesosSimulator;
import simulator.OmegaSimulator;
import simulator.core.CellState;
import simulator.core.Job;
import simulator.core.Workload;

import java.io.PrintWriter;
import java.util.*;

/**
 * A simple experiment runner that runs three small simulations (Monolithic, Mesos, Omega)
 * and prints a CSV summary to stdout.
 */
public class ExperimentRunner {
    public static void main(String[] args) {
        PrintWriter out = new PrintWriter(System.out, true);
        out.println("experiment,metric,value");

        // Build separate workloads for each simulator to avoid reusing Job objects
        // UNIFIED approach: single mixed workload processed by all schedulers
        List<Workload> prefill = Collections.emptyList();

        List<Workload> workloadsMono = buildUnifiedWorkload();
        List<Workload> workloadsMesos = buildUnifiedWorkload();
        List<Workload> workloadsOmega = buildUnifiedWorkload();

        // Different think times per workload type
        Map<String, Double> constantThink = new HashMap<>();
        Map<String, Double> perTaskThink = new HashMap<>();
        constantThink.put("Mixed", 0.15);
        perTaskThink.put("Mixed", 0.015);

        // 1) Monolithic (single centralized scheduler)
        try {
            // EXTREME constraint: 2 machines, minimal resources
            CellState csMono = new CellState(2, 4.0, 8000.0, "sequence-numbers", "incremental");
            MonolithicScheduler mono = new MonolithicScheduler("monolithic", constantThink, perTaskThink, 0);
            Map<String, scheduler.IScheduler> schedsMono = new HashMap<>();
            schedsMono.put(mono.getName(), mono);
            Map<String, List<String>> mappingMono = new HashMap<>();
            mappingMono.put("Mixed", Arrays.asList(mono.getName()));

            ClusterSimulator simMono = new ClusterSimulator(csMono, schedsMono, mappingMono, workloadsMono, prefill, false);
            simMono.run(null, null);

            out.println(String.format("monolithic,num_successful_transactions,%d", mono.getNumSuccessfulTransactions()));
            out.println(String.format("monolithic,total_useful_time_scheduling,%.3f", mono.getTotalUsefulTimeScheduling()));
            out.println(String.format("monolithic,num_failed_transactions,%d", mono.getNumFailedTransactions()));
            out.println(String.format("monolithic,num_jobs_timed_out,%d", mono.getNumJobsTimedOutScheduling()));
        } catch (Exception e) {
            out.println("monolithic,error," + e.getMessage());
            e.printStackTrace(out);
        }

        // 2) Mesos (resource-offer based)
        try {
            CellState csMesos = new CellState(2, 4.0, 8000.0, "resource-fit", "incremental");
            // Use DRF allocator with frequent offer generation
            MesosAllocator allocator = new MesosAllocator(0.001, 1.0, 1.0, 0.5);
            MesosScheduler mesos = new MesosScheduler("mesos", constantThink, perTaskThink, true, 0);
            Map<String, MesosScheduler> schedsMesos = new HashMap<>();
            schedsMesos.put(mesos.getName(), mesos);
            Map<String, List<String>> mappingMesos = new HashMap<>();
            mappingMesos.put("Mixed", Arrays.asList(mesos.getName()));

            MesosSimulator simMesos = new MesosSimulator(csMesos, schedsMesos, mappingMesos, workloadsMesos, prefill, allocator, false);
            simMesos.run(null, null);

            out.println(String.format("mesos,num_successful_transactions,%d", mesos.getNumSuccessfulTransactions()));
            out.println(String.format("mesos,total_useful_time_scheduling,%.3f", mesos.getTotalUsefulTimeScheduling()));
            out.println(String.format("mesos,num_failed_transactions,%d", mesos.getNumFailedTransactions()));
            out.println(String.format("mesos,num_jobs_timed_out,%d", mesos.getNumJobsTimedOutScheduling()));
        } catch (Exception e) {
            out.println("mesos,error," + e.getMessage());
            e.printStackTrace(out);
        }

        // 3) Omega (optimistic concurrency control with parallel schedulers)
        try {
            CellState csOmega = new CellState(2, 4.0, 8000.0, "sequence-numbers", "all-or-nothing");
            // Create separate schedulers for different workload types (enable parallel scheduling)
            OmegaScheduler omegaBatch = new OmegaScheduler("omega-batch", constantThink, perTaskThink, 0);
            OmegaScheduler omegaInteractive = new OmegaScheduler("omega-interactive", constantThink, perTaskThink, 0);
            OmegaScheduler omegaService = new OmegaScheduler("omega-service", constantThink, perTaskThink, 0);
            
            Map<String, OmegaScheduler> schedsOmega = new HashMap<>();
            schedsOmega.put(omegaBatch.getName(), omegaBatch);
            schedsOmega.put(omegaInteractive.getName(), omegaInteractive);
            schedsOmega.put(omegaService.getName(), omegaService);
            
            Map<String, List<String>> mappingOmega = new HashMap<>();
            // ALL THREE schedulers can process the same Mixed workload
            // This forces them to compete for the same job queue, generating conflicts!
            mappingOmega.put("Mixed", Arrays.asList(
                omegaBatch.getName(), 
                omegaInteractive.getName(), 
                omegaService.getName()));

            OmegaSimulator simOmega = new OmegaSimulator(csOmega, schedsOmega, mappingOmega, workloadsOmega, prefill, false);
            simOmega.run(null, null);

            // Aggregate stats from all three schedulers
            long totalOmegaSuccess = omegaBatch.getNumSuccessfulTransactions() + 
                                     omegaInteractive.getNumSuccessfulTransactions() + 
                                     omegaService.getNumSuccessfulTransactions();
            double totalOmegaTime = omegaBatch.getTotalUsefulTimeScheduling() + 
                                    omegaInteractive.getTotalUsefulTimeScheduling() + 
                                    omegaService.getTotalUsefulTimeScheduling();
            long totalOmegaFailed = omegaBatch.getNumFailedTransactions() + 
                                    omegaInteractive.getNumFailedTransactions() + 
                                    omegaService.getNumFailedTransactions();
            long totalOmegaTimedOut = omegaBatch.getNumJobsTimedOutScheduling() + 
                                      omegaInteractive.getNumJobsTimedOutScheduling() + 
                                      omegaService.getNumJobsTimedOutScheduling();
            long totalOmegaRetried = omegaBatch.getNumRetriedTransactions() + 
                                     omegaInteractive.getNumRetriedTransactions() + 
                                     omegaService.getNumRetriedTransactions();

            out.println(String.format("omega,num_successful_transactions,%d", totalOmegaSuccess));
            out.println(String.format("omega,total_useful_time_scheduling,%.3f", totalOmegaTime));
            out.println(String.format("omega,num_failed_transactions,%d", totalOmegaFailed));
            out.println(String.format("omega,num_jobs_timed_out,%d", totalOmegaTimedOut));
            out.println(String.format("omega,num_retried_transactions,%d", totalOmegaRetried));
        } catch (Exception e) {
            out.println("omega,error," + e.getMessage());
            e.printStackTrace(out);
        }
    }

    /**
     * Build a UNIFIED mixed workload with high density job arrivals.
     * All schedulers will process this same workload, forcing Omega's three
     * schedulers to compete for the same job queue and resources.
     */
    private static List<Workload> buildUnifiedWorkload() {
        List<Workload> workloads = new ArrayList<>();
        
        // Single "Mixed" workload with diverse tasks arriving very densely
        Workload mixed = new Workload("Mixed");
        
        // 180 jobs of mixed characteristics arriving in 0.4s
        for (int i = 0; i < 180; i++) {
            int numTasks;
            double cpuPerTask;
            double memPerTask;
            
            // Vary task sizes to simulate realistic workload mix
            if (i % 3 == 0) {
                // ~60 "large" batch-like jobs
                numTasks = 2;
                cpuPerTask = 1.0;
                memPerTask = 2000.0;
            } else if (i % 3 == 1) {
                // ~60 "medium" interactive-like jobs
                numTasks = 2;
                cpuPerTask = 1.0;
                memPerTask = 1500.0;
            } else {
                // ~60 "small" service-like jobs
                numTasks = 1;
                cpuPerTask = 0.6;
                memPerTask = 1000.0;
            }
            
            // All jobs arrive very densely: 0.005s apart = 180 jobs in 0.9s
            Job j = new Job(1000 + i, i * 0.005, numTasks, 50.0, "Mixed", cpuPerTask, memPerTask);
            mixed.addJob(j);
        }
        workloads.add(mixed);
        return workloads;
    }
}
