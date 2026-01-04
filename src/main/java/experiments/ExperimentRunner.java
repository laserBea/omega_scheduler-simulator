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

        // Common workload: a single workload with several small jobs
        Workload wl = new Workload("wl");
        for (int i = 0; i < 20; i++) {
            // id, submitted, numTasks, taskDuration, workloadName, cpusPerTask, memPerTask
            Job j = new Job(i + 1, i * 0.5, 1, 50.0, "wl", 1.0, 1000.0);
            wl.addJob(j);
        }

        List<Workload> workloads = Collections.singletonList(wl);
        List<Workload> prefill = Collections.emptyList();

        Map<String, Double> constantThink = new HashMap<>();
        Map<String, Double> perTaskThink = new HashMap<>();
        constantThink.put("wl", 0.1);
        perTaskThink.put("wl", 0.01);

        // 1) Monolithic
        try {
            CellState csMono = new CellState(10, 8.0, 16000.0, "sequence-numbers", "incremental");
            MonolithicScheduler mono = new MonolithicScheduler("monolithic", constantThink, perTaskThink, 0);
            Map<String, scheduler.IScheduler> schedsMono = new HashMap<>();
            schedsMono.put(mono.getName(), mono);
            Map<String, List<String>> mappingMono = new HashMap<>();
            mappingMono.put("wl", Arrays.asList(mono.getName()));

            ClusterSimulator simMono = new ClusterSimulator(csMono, schedsMono, mappingMono, workloads, prefill, false);
            simMono.run(null, null);

            out.println(String.format("monolithic,num_successful_transactions,%d", mono.getNumSuccessfulTransactions()));
            out.println(String.format("monolithic,total_useful_time_scheduling,%.3f", mono.getTotalUsefulTimeScheduling()));
        } catch (Exception e) {
            out.println("monolithic,error," + e.getMessage());
        }

        // 2) Mesos
        try {
            CellState csMesos = new CellState(10, 8.0, 16000.0, "resource-fit", "incremental");
            MesosAllocator allocator = new MesosAllocator(0.01, 1.0, 1.0, 0.5);
            MesosScheduler mesos = new MesosScheduler("mesos", constantThink, perTaskThink, true, 0);
            Map<String, MesosScheduler> schedsMesos = new HashMap<>();
            schedsMesos.put(mesos.getName(), mesos);
            Map<String, List<String>> mappingMesos = new HashMap<>();
            mappingMesos.put("wl", Arrays.asList(mesos.getName()));

            MesosSimulator simMesos = new MesosSimulator(csMesos, schedsMesos, mappingMesos, workloads, prefill, allocator, false);
            simMesos.run(null, null);

            out.println(String.format("mesos,num_successful_transactions,%d", mesos.getNumSuccessfulTransactions()));
            out.println(String.format("mesos,total_useful_time_scheduling,%.3f", mesos.getTotalUsefulTimeScheduling()));
        } catch (Exception e) {
            out.println("mesos,error," + e.getMessage());
        }

        // 3) Omega
        try {
            CellState csOmega = new CellState(10, 8.0, 16000.0, "sequence-numbers", "all-or-nothing");
            OmegaScheduler omega = new OmegaScheduler("omega", constantThink, perTaskThink, 0);
            Map<String, OmegaScheduler> schedsOmega = new HashMap<>();
            schedsOmega.put(omega.getName(), omega);
            Map<String, List<String>> mappingOmega = new HashMap<>();
            mappingOmega.put("wl", Arrays.asList(omega.getName()));

            OmegaSimulator simOmega = new OmegaSimulator(csOmega, schedsOmega, mappingOmega, workloads, prefill, false);
            simOmega.run(null, null);

            out.println(String.format("omega,num_successful_transactions,%d", omega.getNumSuccessfulTransactions()));
            out.println(String.format("omega,total_useful_time_scheduling,%.3f", omega.getTotalUsefulTimeScheduling()));
        } catch (Exception e) {
            out.println("omega,error," + e.getMessage());
        }
    }
}
