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

        Map<String, Double> constantThink = new HashMap<>();
        Map<String, Double> perTaskThink = new HashMap<>();
        // Higher thinking times to amplify concurrency advantages
        constantThink.put("wl", 1.0);
        perTaskThink.put("wl", 0.1);

        // 1) Monolithic
        try {
            // Create fresh workload for Monolithic
            Workload wlMono = new Workload("wl");
            Random random = new Random(42);
            for (int i = 0; i < 300; i++) {
                int numTasks = 12 + random.nextInt(12);
                double submitTime = 0.0; // All jobs arrive simultaneously for maximum concurrency
                Job j = new Job(i + 1, submitTime, numTasks, 1.0, "wl", 1.0, 400.0);
                wlMono.addJob(j);
            }
            List<Workload> workloadsMono = Collections.singletonList(wlMono);
            List<Workload> prefillMono = Collections.emptyList();

            CellState csMono = new CellState(2, 8.0, 12000.0, "sequence-numbers", "incremental");
            MonolithicScheduler mono = new MonolithicScheduler("monolithic", constantThink, perTaskThink, 0);
            Map<String, scheduler.IScheduler> schedsMono = new HashMap<>();
            schedsMono.put(mono.getName(), mono);
            Map<String, List<String>> mappingMono = new HashMap<>();
            mappingMono.put("wl", Arrays.asList(mono.getName()));

            ClusterSimulator simMono = new ClusterSimulator(csMono, schedsMono, mappingMono, workloadsMono, prefillMono, false);
            simMono.run(null, null);

            out.println(String.format("monolithic,num_successful_transactions,%d", mono.getNumSuccessfulTransactions()));
            out.println(String.format("monolithic,num_retried_transactions,%d", mono.getNumRetriedTransactions()));
            out.println(String.format("monolithic,total_useful_time_scheduling,%.3f", mono.getTotalUsefulTimeScheduling()));
        } catch (Exception e) {
            out.println("monolithic,error," + e.getMessage());
        }

        // 2) Mesos
        try {
            // Create fresh workload for Mesos
            Workload wlMesos = new Workload("wl");
            Random random = new Random(42);
            for (int i = 0; i < 300; i++) {
                int numTasks = 12 + random.nextInt(12);
                double submitTime = 0.0; // All jobs arrive simultaneously for maximum concurrency
                Job j = new Job(i + 1, submitTime, numTasks, 1.0, "wl", 1.0, 400.0);
                wlMesos.addJob(j);
            }
            List<Workload> workloadsMesos = Collections.singletonList(wlMesos);
            List<Workload> prefillMesos = Collections.emptyList();

            CellState csMesos = new CellState(2, 8.0, 12000.0, "resource-fit", "incremental");
            MesosAllocator allocator = new MesosAllocator(0.1, 2.0, 2000.0, 1.0);
            MesosScheduler mesos = new MesosScheduler("mesos", constantThink, perTaskThink, true, 0);
            Map<String, MesosScheduler> schedsMesos = new HashMap<>();
            schedsMesos.put(mesos.getName(), mesos);
            Map<String, List<String>> mappingMesos = new HashMap<>();
            mappingMesos.put("wl", Arrays.asList(mesos.getName()));

            MesosSimulator simMesos = new MesosSimulator(csMesos, schedsMesos, mappingMesos, workloadsMesos, prefillMesos, allocator, false);
            simMesos.run(null, null);

            out.println(String.format("mesos,num_successful_transactions,%d", mesos.getNumSuccessfulTransactions()));
            out.println(String.format("mesos,num_retried_transactions,%d", mesos.getNumRetriedTransactions()));
            out.println(String.format("mesos,total_useful_time_scheduling,%.3f", mesos.getTotalUsefulTimeScheduling()));
        } catch (Exception e) {
            out.println("mesos,error," + e.getMessage());
        }

        // 3) Omega
        try {
            // Create fresh workload for Omega
            Workload wlOmega = new Workload("wl");
            Random random = new Random(42);
            for (int i = 0; i < 300; i++) {
                int numTasks = 12 + random.nextInt(12);
                double submitTime = 0.0; // All jobs arrive simultaneously for maximum concurrency
                Job j = new Job(i + 1, submitTime, numTasks, 1.0, "wl", 1.0, 400.0);
                wlOmega.addJob(j);
            }
            List<Workload> workloadsOmega = Collections.singletonList(wlOmega);
            List<Workload> prefillOmega = Collections.emptyList();

            CellState csOmega = new CellState(2, 8.0, 12000.0, "sequence-numbers", "all-or-nothing");
            OmegaScheduler omega = new OmegaScheduler("omega", constantThink, perTaskThink, 0);
            Map<String, OmegaScheduler> schedsOmega = new HashMap<>();
            schedsOmega.put(omega.getName(), omega);
            Map<String, List<String>> mappingOmega = new HashMap<>();
            mappingOmega.put("wl", Arrays.asList(omega.getName()));

            OmegaSimulator simOmega = new OmegaSimulator(csOmega, schedsOmega, mappingOmega, workloadsOmega, prefillOmega, false);
            simOmega.run(null, null);

            out.println(String.format("omega,num_successful_transactions,%d", omega.getNumSuccessfulTransactions()));
            out.println(String.format("omega,num_retried_transactions,%d", omega.getNumRetriedTransactions()));
            out.println(String.format("omega,total_useful_time_scheduling,%.3f", omega.getTotalUsefulTimeScheduling()));
        } catch (Exception e) {
            out.println("omega,error," + e.getMessage());
        }
    }
}