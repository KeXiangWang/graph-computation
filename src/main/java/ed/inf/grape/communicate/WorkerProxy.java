package ed.inf.grape.communicate;

import java.rmi.AccessException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ed.inf.discovery.Pattern;
import ed.inf.grape.core.Coordinator;
import ed.inf.grape.core.Worker;
import ed.inf.grape.graph.Partition;

/**
 * Represents a thread which is used by the master to talk to workers and
 * vice-versa.
 * 
 * @author Yecol
 */

public class WorkerProxy implements Worker2Coordinator {

	private static final long serialVersionUID = 3730860769731338654L;

	/** The worker. */
	private Worker worker;

	/** The coordinator. */
	private Coordinator coordinator;

	/** The thread */
	private Thread t;

	/** The num worker threads. */
	private int numWorkerThreads;

	/** The worker id. */
	String workerID;

	/** The partition list. */
	BlockingQueue<Partition> partitionList;

	/** The partition list. */
	BlockingQueue<Integer> partitionIDList;

	/** The total partitions. */
	private int totalPartitions = 0;

	static Logger log = LogManager.getLogger(WorkerProxy.class);

	/**
	 * Instantiates a new worker proxy.
	 */

	public WorkerProxy(Worker worker, String workerID, int numWorkerThreads,
			Coordinator coordinator) throws AccessException, RemoteException {
		this.worker = worker;
		this.workerID = workerID;
		this.numWorkerThreads = numWorkerThreads;
		this.coordinator = coordinator;
	}

	/**
	 * Exit.
	 */
	public void exit() {
		try {
			t.interrupt();
		} catch (Exception e) {
			System.out.println("Worker Stopped");
		}
	}

	/**
	 * Gets the num threads.
	 * 
	 * @return the num threads
	 */
	public int getNumThreads() {
		return numWorkerThreads;
	}

	/**
	 * Halts the worker and prints the final solution.
	 */
	public void halt() throws RemoteException {
		this.restoreInitialState();
		worker.halt();
	}

	/**
	 * Adds the partition.
	 * 
	 * @param partition
	 *            the partition
	 */
	public void addPartitionID(int partitionID) {

		totalPartitions += 1;
		partitionIDList.add(partitionID);
	}

	/**
	 * Adds the partition list.
	 * 
	 * @param workerPartitions
	 *            the worker partitions
	 */
	public void addPartitionIDList(List<Integer> workerPartitionIDs) {
		try {
			totalPartitions += workerPartitionIDs.size();
			worker.addPartitionIDList(workerPartitionIDs);
		} catch (RemoteException e) {
			log.fatal("Remote Exception received from the Worker.");
			log.fatal("Giving back the partition to the Master.");

			e.printStackTrace();
			// give the partition back to Master
			coordinator.removeWorker(workerID);
			return;
		}
	}

	/**
	 * Sets the worker partition info.
	 * 
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void setWorkerPartitionInfo(
			Map<Integer, Integer> vertexIdToPartitionId,
			Map<Integer, String> mapPartitionIdToWorkerId,
			Map<String, Worker> mapWorkerIdToWorker) throws RemoteException {

		log.debug("workerProxy.totalPartitions=" + totalPartitions);

		worker.setWorkerPartitionInfo(totalPartitions, vertexIdToPartitionId,
				mapPartitionIdToWorkerId, mapWorkerIdToWorker);
	}

	/**
	 * Sets Query to
	 */

	public void setQuery(String query) throws RemoteException {
		worker.setQuery(query);
	}

	/**
	 * Gets the worker id.
	 * 
	 * @return the worker id
	 */
	public String getWorkerID() {
		return workerID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see system.Worker2Master#register(system.Worker, java.lang.String, int)
	 */
	@Override
	public Worker2Coordinator register(Worker worker, String workerID,
			int numWorkerThreads) throws RemoteException {
		return null;
	}

	/**
	 * Restore initial state.
	 */
	private void restoreInitialState() {
		this.totalPartitions = 0;
	}

	/**
	 * Shutdowns the worker and exits
	 */
	@Override
	public void shutdown() {
		try {
			worker.shutdown();
		} catch (RemoteException e) {
			this.exit();
		}
	}

	// @Override
	// public void localComputeCompleted(String workerID,
	// Set<String> activeWorkerIDs) throws RemoteException {
	// this.coordinator.localComputeCompleted(workerID, activeWorkerIDs);
	// }
	//
	// public void nextLocalCompute(long superstep) throws RemoteException {
	// this.worker.nextLocalCompute(superstep);
	// }
	//
	// public void processPartialResult() throws RemoteException {
	// this.worker.processPartialResult();
	// }
	//
	// @Override
	// public void sendPartialResult(String workerID,
	// Map<Integer, Result> mapPartitionID2Result) throws RemoteException {
	// this.coordinator.receivePartialResults(workerID, mapPartitionID2Result);
	// }

	public void workerRunNextStep(long superstep) throws RemoteException {
		this.worker.nextStep(superstep);
	}

	public void sendMessageWorker2Coordinator(String workerID,
			List<Pattern> messages) throws RemoteException {
		this.coordinator.receiveMessages(workerID, messages);
	}

	public void sendMessageCoordinator2Worker(List<Pattern> messages)
			throws RemoteException {
		this.worker.receiveMessage(messages);
	}
}
