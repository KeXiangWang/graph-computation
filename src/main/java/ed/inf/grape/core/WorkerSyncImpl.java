package ed.inf.grape.core;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.discovery.DiscoveryTask;
import ed.inf.discovery.Pattern;
import ed.inf.grape.communicate.Worker2Coordinator;
import ed.inf.grape.communicate.Worker2WorkerProxy;
import ed.inf.grape.graph.Partition;
import ed.inf.grape.interfaces.Result;
import ed.inf.grape.util.Dev;
import ed.inf.grape.util.IO;
import ed.inf.grape.util.KV;

/**
 * Represents the computation node.
 *
 * @author Yecol
 */

public class WorkerSyncImpl extends UnicastRemoteObject implements Worker {

    private static final long serialVersionUID = 8653095027537771705L;

    /**
     * The number of threads.
     */
    private int numThreads;

    /**
     * The total partitions assigned.
     */
    private int totalPartitionsAssigned;

    /**
     * The queue of partitions in the current super step.
     */
    private BlockingQueue<DiscoveryTask> currentTasksQueue;

    /**
     * The queue of partitions in the next super step.
     */
    private BlockingQueue<DiscoveryTask> nextTasksQueue;

    /**
     * hosting partitions
     */
    private Map<Integer, Partition> partitions;

    /**
     * Host name of the node with time stamp information.
     */
    private String workerID;

    /**
     * Coordinator Proxy object to interact with Master.
     */
    private Worker2Coordinator coordinatorProxy;

    /** VertexID 2 PartitionID Map */
    // private Map<Integer, Integer> mapVertexIdToPartitionId;

    /**
     * PartitionID to WorkerID Map.
     */
    private Map<Integer, String> mapPartitionIdToWorkerId;

    /**
     * Worker2WorkerProxy Object.
     */
    private Worker2WorkerProxy worker2WorkerProxy;

    /**
     * Worker to Outgoing Messages Map.
     */
    // private ConcurrentHashMap<String, List<UpMessage>> outgoingMessages;
    private ConcurrentLinkedQueue<Pattern> outgoingMessages;

    /**
     * PartitionID to Outgoing Results Map.
     */
    private ConcurrentHashMap<Integer, Result> partialResults;

    /**
     * partitionId to Previous Incoming messages - Used in current Super Step.
     */
    private List<Pattern> previousIncomingMessages;

    /**
     * partitionId to Current Incoming messages - used in next Super Step.
     */
    private ConcurrentLinkedQueue<Pattern> currentIncomingMessages;

    /**
     * boolean variable indicating whether the partitions can be worked upon by
     * the workers in each super step.
     **/
    private boolean flagLocalCompute = false;
    /**
     * boolean variable to determine if a Worker can send messages to other
     * Workers and to Master. It is set to true when a Worker is sending
     * messages to other Workers.
     */
    private boolean stopSendingMessage = false;

    // private boolean flagLastStep = false;

    /**
     * The super step counter.
     */
    private long superstep = 0;

    static Logger log = LogManager.getLogger(WorkerSyncImpl.class);

    /**
     * Instantiates a new worker.
     *
     * @throws RemoteException the remote exception
     */
    public WorkerSyncImpl() throws RemoteException {
        InetAddress address = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
                "MMdd.HHmmss.SSS");
        String timestamp = simpleDateFormat.format(new Date());
        String hostName = new String();
        try {
            address = InetAddress.getLocalHost();
            hostName = address.getHostName();
        } catch (UnknownHostException e) {
            hostName = "UnKnownHost";
            log.error(e);
        }

        this.workerID = hostName + "_" + timestamp;
        this.partitions = new HashMap<Integer, Partition>();
        this.currentTasksQueue = new LinkedBlockingDeque<DiscoveryTask>();
        this.nextTasksQueue = new LinkedBlockingQueue<DiscoveryTask>();
        this.currentIncomingMessages = new ConcurrentLinkedQueue<Pattern>();
        this.partialResults = new ConcurrentHashMap<Integer, Result>();
        this.previousIncomingMessages = new LinkedList<Pattern>();
        this.outgoingMessages = new ConcurrentLinkedQueue<Pattern>();
        this.numThreads = Math.min(Runtime.getRuntime().availableProcessors(),
                KV.MAX_THREAD_LIMITATION);

        for (int i = 0; i < numThreads; i++) {
            log.debug("Starting AsyncThread " + (i + 1));
            WorkerThread workerThread = new WorkerThread();
            workerThread.setName(this.workerID + "_th" + i);
            workerThread.start();
        }

    }

    @Override
    public void addPartitionIDList(List<Integer> workerPartitionIDs)
            throws RemoteException {

        for (int partitionID : workerPartitionIDs) {

            if (!this.partitions.containsKey(partitionID)) {

                String filename = KV.GRAPH_FILE_PATH_PREFIX + "-"
                        + String.valueOf(partitionID);

                Partition partition;
                // log.info("FROM  WorkerSyncImpl addPartitionIDList /////////////////////");
                partition = IO.loadPartitionFromVEFile(partitionID, filename);
                this.partitions.put(partitionID, partition);
            }
        }

        log.debug(Dev.currentRuntimeState());

    }

    @Override
    public void addPartitionID(int partitionID) throws RemoteException {

        if (!this.partitions.containsKey(partitionID)) {

            String filename = KV.GRAPH_FILE_PATH_PREFIX + "-"
                    + String.valueOf(partitionID);
            // log.info("FROM  WorkerSyncImpl addPartitionID ////////////////////");
            Partition partition;
            partition = IO.loadPartitionFromVEFile(partitionID, filename);
            this.partitions.put(partitionID, partition);
        }

        log.debug(Dev.currentRuntimeState());

    }

    /**
     * Gets the num threads.
     *
     * @return the num threads
     */
    @Override
    public int getNumThreads() {
        return numThreads;
    }

    /**
     * Gets the worker id.
     *
     * @return the worker id
     */
    @Override
    public String getWorkerID() {
        return workerID;
    }

    /**
     * The Class SyncWorkerThread.
     */
    private class WorkerThread extends Thread {

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
                while (flagLocalCompute) {
                    // log.debug(this + "superstep loop start for superstep "
//							+ superstep);
                    try {

                        DiscoveryTask task = currentTasksQueue.take();
                        Partition workingPartition = partitions.get(task
                                .getPartitionID());
                        task.setSuperstep(superstep);

                        if (superstep == 0) {

                            /** begin step. initial compute */
                            task.startStep(workingPartition);
//
//							for(Pattern p : task.getMessages()){
//								System.out.println(p);
//                                log.debug(" --------------------------------------------  " + p);
//                                log.debug("  start pattern " + p.getXCandidates());
//							}

//                            log.debug(workingPartition +" --------------------------------------------  "+
//                                    task.getMessages());
                            updateOutgoingMessages(task.getMessages());
                        } else {

                            /** not begin step. incremental compute */
                            // TODO: deal with received messages;
                            // List<DownMessage> messageForWorkingPartition =
                            // previousIncomingMessages
                            // .get(task.getPartitionID());
                            //
                            // if (messageForWorkingPartition != null) {
                            //
                            // task.continuesStep(workingPartition,
                            // messageForWorkingPartition);
                            // updateOutgoingMessages(task.getMessages());
                            // }
//                            log.debug(workingPartition +"  =======================     ==================  "+
//                                    previousIncomingMessages);
                            task.continuesStep(workingPartition, previousIncomingMessages);
//                            for(Pattern p : task.getMessages()){
//                                 log.debug(" --------------------------------------------  " + p);
//                            }
                            updateOutgoingMessages(task.getMessages());
                        }

                        task.prepareForNextCompute();

                        nextTasksQueue.add(task);
                        checkAndSendMessage();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Check and send message. Notice: this is a critical code area, which
     * should put outside of the thread code.
     *
     * @throws RemoteException
     */

    private synchronized void checkAndSendMessage() {

        log.debug("synchronized checkAndSendMessage!");
        if ((!stopSendingMessage)
                && (nextTasksQueue.size() == totalPartitionsAssigned)) {
            log.debug("sendMessage!");

            stopSendingMessage = true;
            flagLocalCompute = false;

            log.debug("Worker: Superstep " + superstep + " completed.");
            log.debug("Worker: outgoing message size = "
                    + outgoingMessages.size());

            long start = System.currentTimeMillis();

            try {

                List<Pattern> messageList = new ArrayList<Pattern>();
                messageList.addAll(outgoingMessages);
                outgoingMessages.clear();
                coordinatorProxy.sendMessageWorker2Coordinator(this.workerID,
                        messageList);

            } catch (RemoteException e) {
                e.printStackTrace();
            }

            log.debug("send up message using "
                    + (System.currentTimeMillis() - start) + "ms.");
        }
    }

    /**
     * Halts the run for this application and prints the output in a file.
     *
     * @throws RemoteException the remote exception
     */
    @Override
    public void halt() throws RemoteException {
        System.out.println("Worker Machine " + workerID + " halts");
        this.restoreInitialState();
    }

    /**
     * Restore the worker to the initial state
     */
    private void restoreInitialState() {
        // this.partitionQueue.clear();
        this.currentIncomingMessages.clear();
        this.outgoingMessages.clear();
        this.mapPartitionIdToWorkerId.clear();
        // this.currentPartitionQueue.clear();
        this.previousIncomingMessages.clear();
        this.stopSendingMessage = false;
        this.flagLocalCompute = false;
        this.totalPartitionsAssigned = 0;
    }

    /**
     * Updates the outgoing messages for every superstep.
     *
     * @param messages FromCompute
     *                 Represents the map of destination vertex and its associated
     *                 message to be send
     */
    private void updateOutgoingMessages(List<Pattern> messages) {

        if (messages != null && !messages.isEmpty()) {
            log.debug("updateOutgoingMessages.size = " + messages.size());
            outgoingMessages.addAll(messages);
        }
    }

    /**
     * Sets the worker partition info.
     *
     * @param totalPartitionsAssigned  the total partitions assigned
     * @param mapVertexIdToPartitionId the map vertex id to partition id
     * @param mapPartitionIdToWorkerId the map partition id id to worker id
     * @param mapWorkerIdToWorker      the map worker id to worker
     * @throws RemoteException
     */
    @Override
    public void setWorkerPartitionInfo(int totalPartitionsAssigned,
                                       Map<Integer, Integer> mapVertexIdToPartitionId,
                                       Map<Integer, String> mapPartitionIdToWorkerId,
                                       Map<String, Worker> mapWorkerIdToWorker) throws RemoteException {
        log.info("WorkerImpl: setWorkerPartitionInfo");
        log.info("totalPartitionsAssigned " + totalPartitionsAssigned
                + " mapPartitionIdToWorkerId: " + mapPartitionIdToWorkerId);
        // log.info("vertex2partitionMapSize: " +
        // mapVertexIdToPartitionId.size());
        this.totalPartitionsAssigned = totalPartitionsAssigned;
        // this.mapVertexIdToPartitionId = mapVertexIdToPartitionId;
        this.mapPartitionIdToWorkerId = mapPartitionIdToWorkerId;
        this.worker2WorkerProxy = new Worker2WorkerProxy(mapWorkerIdToWorker);
    }

    /**
     * The main method.
     *
     * @param args the arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        try {
            String coordinatorMachineName = args[0];
            log.info("masterMachineName " + coordinatorMachineName);

            String masterURL = "//" + coordinatorMachineName + "/"
                    + KV.COORDINATOR_SERVICE_NAME;
            Worker2Coordinator worker2Coordinator = (Worker2Coordinator) Naming
                    .lookup(masterURL);
            Worker worker = new WorkerSyncImpl();
            Worker2Coordinator coordinatorProxy = worker2Coordinator.register(
                    worker, worker.getWorkerID(), worker.getNumThreads());

            worker.setCoordinatorProxy(coordinatorProxy);
            log.info("Worker is bound and ready for computations ");

        } catch (Exception e) {
            log.error("ComputeEngine exception:");
            e.printStackTrace();
        }
    }

    /**
     * Sets the master proxy.
     *
     * @param coordinatorProxy masterProxy
     *                         the new master proxy
     */
    @Override
    public void setCoordinatorProxy(Worker2Coordinator coordinatorProxy) {
        this.coordinatorProxy = coordinatorProxy;
    }

    /**
     * Receive message.
     *
     * @param incomingMessages the incoming messages
     * @throws RemoteException the remote exception
     */
    @Override
    public void receiveMessage(List<Pattern> incomingMessages)
            throws RemoteException {

        log.debug("onRecevieIncomingMessages: " + incomingMessages.size());

        currentIncomingMessages.addAll(incomingMessages);
    }

    /**
     * shutdown the worker
     */
    @Override
    public void shutdown() throws RemoteException {
        java.util.Date date = new java.util.Date();
        log.info("Worker" + workerID + " goes down now at :"
                + new Timestamp(date.getTime()));
        System.exit(0);
    }

    @Override
    public void nextStep(long superstep) throws RemoteException {

        /**
         * Next local compute. No generated new local compute tasks. Transit
         * compute task and status from the last step.
         * */

        this.superstep = superstep;

        // Put all elements in current incoming queue to previous incoming queue
        // and clear the current incoming queue.
        this.previousIncomingMessages.clear();
        this.previousIncomingMessages.addAll(this.currentIncomingMessages);
        this.currentIncomingMessages.clear();

        this.stopSendingMessage = false;
        this.flagLocalCompute = true;

        this.outgoingMessages.clear();

        // Put all local compute tasks in current task queue.
        // clear the completed partitions.
        // Note: To avoid concurrency issues, it is very important that
        // completed partitions is cleared before the Worker threads start to
        // operate on the partition queue in the next super step
        BlockingQueue<DiscoveryTask> temp = new LinkedBlockingDeque<DiscoveryTask>(
                nextTasksQueue);
        this.nextTasksQueue.clear();
        this.currentTasksQueue.addAll(temp);

    }

    @Override
    public void setQuery(String query) throws RemoteException {

        /**
         * Get distributed query from coordinator. and instantiate local compute
         * tasks.
         * */

        log.info("Get query:" + query.toString());

        for (Entry<Integer, Partition> entry : this.partitions.entrySet()) {

            try {

                DiscoveryTask task = new DiscoveryTask(entry.getKey());
                this.nextTasksQueue.add(task);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        log.info("Instantiate " + this.nextTasksQueue.size() + " local task.");
    }

}
