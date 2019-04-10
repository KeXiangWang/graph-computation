package ed.inf.grape.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.graph.DefaultEdge;

import ed.inf.discovery.Pattern;
import ed.inf.discovery.auxiliary.PatternPair;
import ed.inf.discovery.auxiliary.SimpleNode;
import ed.inf.grape.communicate.Client2Coordinator;
import ed.inf.grape.communicate.Worker2Coordinator;
import ed.inf.grape.communicate.WorkerProxy;
import ed.inf.grape.util.Compute;
import ed.inf.grape.util.Dev;
import ed.inf.grape.util.KV;

/**
 * The Class Coordinator.
 * 
 * @author yecol
 */
public class Coordinator extends UnicastRemoteObject implements Worker2Coordinator,
		Client2Coordinator {

	private static final long serialVersionUID = 7264167926318903124L;

	/** The total number of worker threads. */
	private static AtomicInteger totalWorkerThreads = new AtomicInteger(0);

	/** The workerID to WorkerProxy map. */
	private Map<String, WorkerProxy> workerProxyMap = new ConcurrentHashMap<String, WorkerProxy>();

	/** The workerID to Worker map. **/
	private Map<String, Worker> workerMap = new HashMap<String, Worker>();

	/** The partitionID to workerID map. **/
	private Map<Integer, String> partitionWorkerMap;

	/** Set of Workers maintained for acknowledgement. */
	private Set<String> workerAcknowledgementSet = new HashSet<String>();

	/** Set of workers who will be active in the next super step. */
	private Set<String> activeWorkerSet = new HashSet<String>();

	private Map<Integer, List<Pattern>> receivedMessages = new HashMap<Integer, List<Pattern>>();

	/** Merged Message **/
	private List<Pattern> deltaE = new LinkedList<Pattern>();
	private List<Pattern> Sigma = new LinkedList<Pattern>();
	// private List<Pattern> listK = new ArrayList<Pattern>();
	private PriorityQueue<PatternPair> listK = new PriorityQueue<PatternPair>();

	// private double[][] diffM = new double[KV.PARAMETER_K][KV.PARAMETER_K];
	// private double bf;

	/** The start time. */
	long startTime;
	long superstep = 0;

	double minF = Double.MAX_VALUE;
	// Pattern minP1IndexTopK;
	// Pattern minP2IndexTopK;

	double maxUconfSigma = 0.0;
	double maxUconfDeltaE = 0.0;

	/** notYCount/YCount */
	// int YCount = 0;
	// int NotYCount = 0;
	double coff = 0.0;

	private static int currentConsistentPatternID = 0;
	static Logger log = LogManager.getLogger(Coordinator.class);

	/**
	 * Instantiates a new coordinator.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 * @throws PropertyNotFoundException
	 *             the property not found exception
	 */
	public Coordinator() throws RemoteException {
		super();

	}

	/**
	 * Gets the active worker set.
	 * 
	 * @return the active worker set
	 */
	public Set<String> getActiveWorkerSet() {
		return activeWorkerSet;
	}

	/**
	 * Sets the active worker set.
	 * 
	 * @param activeWorkerSet
	 *            the new active worker set
	 */
	public void setActiveWorkerSet(Set<String> activeWorkerSet) {
		this.activeWorkerSet = activeWorkerSet;
	}

	/**
	 * Registers the worker computation nodes with the master.
	 * 
	 * @param worker
	 *            Represents the {@link WorkerSyncImpl.WorkerImpl Worker}
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            Represents the number of worker threads available in the
	 *            worker computation node
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public Worker2Coordinator register(Worker worker, String workerID, int numWorkerThreads)
			throws RemoteException {

		log.debug("Coordinator: Register");
		totalWorkerThreads.getAndAdd(numWorkerThreads);
		WorkerProxy workerProxy = new WorkerProxy(worker, workerID, numWorkerThreads, this);
		workerProxyMap.put(workerID, workerProxy);
		workerMap.put(workerID, worker);
		return (Worker2Coordinator) UnicastRemoteObject.exportObject(workerProxy, 0);
	}

	/**
	 * Gets the worker proxy map info.
	 * 
	 * @return Returns the worker proxy map info
	 */
	public Map<String, WorkerProxy> getWorkerProxyMap() {
		return workerProxyMap;
	}

	/**
	 * Send worker partition info.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void sendWorkerPartitionInfo() throws RemoteException {
		log.debug("Coordinator: sendWorkerPartitionInfo");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setWorkerPartitionInfo(null, partitionWorkerMap, workerMap);
		}
	}

	public void sendQuery(String query) throws RemoteException {
		log.debug("Coordinator: sendWorkerPartitionInfo");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setQuery(query);
		}
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		System.setSecurityManager(new RMISecurityManager());
		Coordinator coordinator;
		try {
			coordinator = new Coordinator();
			Registry registry = LocateRegistry.createRegistry(KV.RMI_PORT);
			registry.rebind(KV.COORDINATOR_SERVICE_NAME, coordinator);
			String resultFolder = (new SimpleDateFormat("yyyyMMdd-hh-mm-ss")).format(new Date());
			KV.RESULT_DIR = KV.OUTPUT_DIR + resultFolder;
			(new File(KV.RESULT_DIR)).mkdir();
			System.out.println(KV.ENABLE_OPT);
			log.info("Coordinator instance is bound to " + KV.RMI_PORT + " and ready.");
		} catch (RemoteException e) {
			Coordinator.log.error(e);
			e.printStackTrace();
		}
	}

	/**
	 * Halts all the workers and prints the final solution.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void halt() throws RemoteException {
		// healthManager.exit();
		log.info("Master: halt");
		log.debug("Worker Proxy Map " + workerProxyMap);

		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.halt();
		}

		// healthManager.exit();
		long endTime = System.currentTimeMillis();
		log.info("Time taken: " + (endTime - startTime) + " ms");
		// Restore the system back to its initial state
		restoreInitialState();
	}

	/**
	 * Restore initial state of the system.
	 */
	private void restoreInitialState() {
		this.activeWorkerSet.clear();
		this.workerAcknowledgementSet.clear();
		this.partitionWorkerMap.clear();
		this.superstep = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */

	/**
	 * Removes the worker.
	 * 
	 * @param workerID
	 *            the worker id
	 */
	public void removeWorker(String workerID) {
		workerProxyMap.remove(workerID);
		workerMap.remove(workerID);
	}

	/**
	 * Gets the partition worker map.
	 * 
	 * @return the partition worker map
	 */
	public Map<Integer, String> getPartitionWorkerMap() {
		return partitionWorkerMap;
	}

	/**
	 * Sets the partition worker map.
	 * 
	 * @param partitionWorkerMap
	 *            the partition worker map
	 */
	public void setPartitionWorkerMap(Map<Integer, String> partitionWorkerMap) {
		this.partitionWorkerMap = partitionWorkerMap;
	}

	/**
	 * Defines a deployment convenience to stop each registered.
	 * 
	 * @throws RemoteException
	 *             the remote exception {@link system.Worker Worker} and then
	 *             stops itself.
	 */

	@Override
	public void shutdown() throws RemoteException {
		// if (healthManager != null)
		// healthManager.exit();
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			try {
				workerProxy.shutdown();
			} catch (Exception e) {
				continue;
			}
		}
		java.util.Date date = new java.util.Date();
		log.info("Master goes down now at :" + new Timestamp(date.getTime()));
		System.exit(0);
	}

	public void loadGraph(String graphFilename) throws RemoteException {

		log.info("load Graph = " + graphFilename);

		assignDistributedPartitions();
		sendWorkerPartitionInfo();
	}

	@Override
	public void putTask(String query) throws RemoteException {

		log.info(query);

		/** initiate local compute tasks. */
		sendQuery(query);

		/** begin to compute. */
		nextLocalCompute();
		startTime = System.currentTimeMillis();
	}

	public void assignDistributedPartitions() {

		/**
		 * Graph file has been partitioned, and the partitioned graph have been
		 * distributed to workers.
		 * 
		 * */

		partitionWorkerMap = new HashMap<Integer, String>();

		int currentPartitionID = 0;

		// Assign partitions to workers in the ratio of the number of worker
		// threads that each worker has.
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {

			WorkerProxy workerProxy = entry.getValue();

			// Compute the number of partitions to assign
			int numThreads = workerProxy.getNumThreads();
			double ratio = ((double) (numThreads)) / totalWorkerThreads.get();
			log.info("Worker " + workerProxy.getWorkerID());
			log.info("ThreadNum = " + numThreads + ", Ratio: " + ratio);
			int numPartitionsToAssign = (int) (ratio * KV.PARTITION_COUNT);
			log.info("numPartitionsToAssign: " + numPartitionsToAssign);

			List<Integer> workerPartitionIDs = new ArrayList<Integer>();
			for (int i = 0; i < numPartitionsToAssign; i++) {
				if (currentPartitionID < KV.PARTITION_COUNT) {
					activeWorkerSet.add(entry.getKey());
					log.info("Adding partition  " + currentPartitionID + " to worker "
							+ workerProxy.getWorkerID());
					workerPartitionIDs.add(currentPartitionID);
					partitionWorkerMap.put(currentPartitionID, workerProxy.getWorkerID());
					currentPartitionID++;
				}
			}
			workerProxy.addPartitionIDList(workerPartitionIDs);
		}

		if (currentPartitionID < KV.PARTITION_COUNT) {
			// Add the remaining partitions (if any) in a round-robin fashion.
			Iterator<Map.Entry<String, WorkerProxy>> workerMapIter = workerProxyMap.entrySet()
					.iterator();

			while (currentPartitionID != KV.PARTITION_COUNT) {
				// If the remaining partitions is greater than the number of the
				// workers, start iterating from the beginning again.
				if (!workerMapIter.hasNext()) {
					workerMapIter = workerProxyMap.entrySet().iterator();
				}

				WorkerProxy workerProxy = workerMapIter.next().getValue();

				activeWorkerSet.add(workerProxy.getWorkerID());
				log.info("Adding partition  " + currentPartitionID + " to worker "
						+ workerProxy.getWorkerID());
				partitionWorkerMap.put(currentPartitionID, workerProxy.getWorkerID());
				workerProxy.addPartitionID(currentPartitionID);

				currentPartitionID++;
			}
		}
	}

	/**
	 * Start super step.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public synchronized void nextLocalCompute() throws RemoteException {

		log.info("Coordinator: next local compute. superstep = " + superstep);

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		for (String workerID : this.activeWorkerSet) {
			this.workerProxyMap.get(workerID).sendMessageCoordinator2Worker(this.deltaE);
			this.workerProxyMap.get(workerID).workerRunNextStep(superstep);
		}
		this.activeWorkerSet.clear();
	}

	public void prepareForNextStep() {
		this.receivedMessages.clear();
	}

	public void finishDiscovery() {
		long mineTime = System.currentTimeMillis() - startTime;
		log.info("finishedDiscovery, time = " + mineTime * 1.0 / 1000 + "s.");
	}

	private void increamentalDiverfy() {

		List<Pattern> cpyDeltaE = new LinkedList<Pattern>();
		List<Pattern> cpySigma = new LinkedList<Pattern>();
		cpyDeltaE.addAll(this.deltaE);
		cpySigma.addAll(this.Sigma);

		for (Iterator<Pattern> itE = cpyDeltaE.iterator(); itE.hasNext();) {

			Pattern pInE = itE.next();

			for (Iterator<Pattern> itS = cpySigma.iterator(); itS.hasNext();) {
				Pattern pInS = itS.next();

				// only test if pInS and pInE are not same.
				if (pInE.getPatternID() != pInS.getPatternID()) {

					double f = Compute.computeDashF(pInE, pInS);

					if (listK.size() < KV.PARAMETER_K) {
						listK.add(new PatternPair(pInE, pInS, f));
						minF = listK.peek().getF();
						itE.remove();
						itS.remove();

						break;
					}

					else if (f > minF) {

						listK.poll();
						listK.add(new PatternPair(pInE, pInS, f));
						minF = listK.peek().getF();
						itE.remove();
						itS.remove();
						break;
					}
				}
			}
		}

	}

	private void generateTopK() {

		// filter delta with support.
		this.filterDeltaE();

		// sigma reduction
		this.sigmaReduction();

		// add deltaE into Sigma
		this.Sigma.addAll(this.deltaE);

		log.debug("begin generate topk with " + this.deltaE.size() + " mergedMsg.");
		log.debug(Dev.currentRuntimeState());

		long start = System.currentTimeMillis();

		this.increamentalDiverfy();
		log.debug(Dev.currentRuntimeState());
		log.debug("generate topk time = " + (System.currentTimeMillis() - start) + "ms");

		this.messageReduction();
	}

	private void sigmaReduction() {

		if (KV.ENABLE_OPT && superstep == 0) {

			if (this.Sigma.size() == 0) {
				return;
			}

			for (Pattern p : this.deltaE) {
				if (p.getConfidenceUB() > maxUconfDeltaE) {
					maxUconfDeltaE = p.getConfidenceUB();
				}
			}

			int reductionCount = 0;

			for (Iterator<Pattern> it = this.Sigma.iterator(); it.hasNext();) {
				double f = Compute.computeLemma1(it.next(), maxUconfDeltaE);
				log.debug("sigma reduction f = " + f + ", vs. minf = " + minF);
				if (f < minF) {
					it.remove();
					reductionCount++;
				}
			}

			log.debug("current maxUE = " + maxUconfDeltaE + ", sigma reduction count = "
					+ reductionCount);
		}

	}

	private void messageReduction() {

		System.out.println("kv.enable-opt:" + KV.ENABLE_OPT);
		System.out.println("step:" + superstep);

		if (KV.ENABLE_OPT && superstep == 0) {

			maxUconfSigma = 0;

			for (Pattern p : this.Sigma) {
				if (p.getConfidenceUB() > maxUconfSigma) {
					maxUconfSigma = p.getConfidenceUB();
				}
			}

			int reductionCount = 0;

			for (Iterator<Pattern> it = this.deltaE.iterator(); it.hasNext();) {

				double f = Compute.computeLemma2(it.next(), maxUconfSigma);
				log.debug("delta reduction f = " + f + ", vs. minf = " + minF);
				// if (f < minF && this.deltaE.size() > KV.LEAST_MESSAGE) {
				if (this.deltaE.size() > KV.LEAST_MESSAGE) {
					it.remove();
					reductionCount++;
				}
			}

			log.debug("current maxUS = " + maxUconfSigma + ", message reduction count = "
					+ reductionCount);

		}

	}

	private void filterDeltaE() {

		int oSize = this.deltaE.size();

		log.debug("filter deltaE with support threshold s = " + KV.PARAMETER_ETA
				+ ". before deltaE.size = " + oSize);

		if (superstep == 0) {
			this.coff = this.deltaE.get(0).getNotYCount() * 1.0 / this.deltaE.get(0).getYCount();
			// divide by N = notYcount * Ycount
			// this.coff = 1.0 / this.deltaE.get(0).getYCount() *
			// this.deltaE.get(0).getYCount();
		}

		for (Iterator<Pattern> iterator = this.deltaE.iterator(); iterator.hasNext();) {
			Pattern p = iterator.next();
			log.debug("supportfilter:" + p.newlyMatchXCount + ", t="
					+ KV.PARAMETER_ETA);
			if (p.getXCandidates().toArray().length < KV.PARAMETER_ETA) { // TODO check this, issued by WKX on 4/9
				iterator.remove();
//			if (p.newlyMatchXCount < KV.PARAMETER_ETA) { // DONE for use new xcount to flit
//				iterator.remove();
			} else {
				// replace patternID with consistentID on coordinator
				p.setCoordinatorPatternID(this.getNextConsistentPatternID());
			}
		}

		// log.debug("filtered patterns# = " + (oSize - this.deltaE.size()));

		for (Pattern p : this.deltaE) {
			// log.debug(" ++++++++++++++++++++++++++++++++++++++++++++  " + p);
			Compute.computeConfidence(p, this.coff);
			Compute.computeUBConfidence(p, this.coff);

			// log.debug("conf=" + p.getConfidence() + " conf+=" +
			// p.getConfidenceUB());
		}
	}

	/**
	 * Assemble Messages get from different sites. Do automorphism check and
	 * merge GPARs.
	 * 
	 */
	private void assembleMessages() {

		log.debug("begin assemble" + this.receivedMessages.size());
		int isoTestTimes = 0;
		boolean firstSetFlag = true;
		long start = System.currentTimeMillis();
		this.deltaE.clear();

		for (int curPartitionID : this.receivedMessages.keySet()) {

			// this.printMessageList(this.receivedMessages.get(curPartitionID));

			if (firstSetFlag) {
				this.deltaE.addAll(this.receivedMessages.get(curPartitionID));
				firstSetFlag = false;
			} else {
				for (Pattern nGPAR : this.receivedMessages.get(curPartitionID)) {
					// boolean findFlag = false;
					for (Pattern isoedGPAR : this.deltaE) {
						isoTestTimes++;
						if (Pattern.testSamePattern(isoedGPAR, nGPAR)) {
							Pattern.add(isoedGPAR, nGPAR);
							// findFlag = true;
							break;
						}
					}
					// if (!findFlag) {
					// this.deltaE.add(nGPAR);
					// }
				}
			}
		}

		log.debug("assemble time = " + (System.currentTimeMillis() - start) + "ms, do iso times = "
				+ isoTestTimes);
	}

	public synchronized void receiveMessages(String workerID, List<Pattern> upMessages) {
		log.info("Coordinator received message from worker " + workerID + " message-size: "
				+ upMessages.size());

		for (Pattern m : upMessages) {
			if (!receivedMessages.containsKey(m.getPartitionID())) {
				receivedMessages.put(m.getPartitionID(), new LinkedList<Pattern>());
			}
			receivedMessages.get(m.getPartitionID()).add(m);
		}

		// this.receivedMessages.addAll(upMessages);
		this.workerAcknowledgementSet.remove(workerID);

		log.info("Coordinator received message" + workerID + " down.");
		log.debug(Dev.currentRuntimeState());

		if (this.workerAcknowledgementSet.size() == 0) {

			if (this.receivedMessages.size() == 0) {

				// workers didn't expanded anything.
				this.writeTopKToFile();
				this.finishDiscovery();
			}

			else {

				// receive expanded GPARs
				this.assembleMessages();
				this.generateTopK();
				this.writeTopKToFile();

				// for next expand.

				this.prepareForNextStep();
				this.superstep++;

				// TODO: test if trigger all the workers. if null then finished.
				this.activeWorkerSet.addAll(this.workerMap.keySet());

				try {
					nextLocalCompute();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public int getNextConsistentPatternID() {
		return currentConsistentPatternID++;
	}

	@Override
	public void preProcess() throws RemoteException {
		this.loadGraph(KV.GRAPH_FILE_PATH_PREFIX);
	}

	@Override
	public void postProcess() throws RemoteException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendMessageWorker2Coordinator(String workerID, List<Pattern> messages)
			throws RemoteException {
	}

	@Override
	public void sendMessageCoordinator2Worker(List<Pattern> messages) throws RemoteException {
	}

	public void printMessageList(List<Pattern> list) {
		log.debug("message list size = " + list.size());
		for (Pattern um : list) {
			log.debug(um.toString());
		}
	}

	public void writeTopKToFile() {

		String resultFile = KV.RESULT_DIR + "/" + this.superstep + ".dat";

		PrintWriter writer;
		try {

			writer = new PrintWriter(resultFile);
			writer.println("======================");

			if (this.listK.size() != 0) {

				writer.println("Sigma size = " + Sigma.size());
				writer.println("Using time = " + (System.currentTimeMillis() - startTime) * 1.0
						/ 1000 + "s.");
				writer.println("opt = " + KV.ENABLE_OPT);
				writer.println("fragment = " + this.workerMap.size());

				writer.println("----------------------");
				writer.println("round = " + this.superstep + ", bf = "
						+ Compute.computeBF(this.listK));

				writer.println("======================");

				for (PatternPair pr : this.listK) {
					writer.println("P1_ID: " + pr.getP1().getPatternID());
					for (SimpleNode _v : pr.getP1().getQ().vertexSet()) {
						StringBuffer _s = new StringBuffer();

						_s.append(_v.nodeID).append("\t").append(_v.attribute);
						for (DefaultEdge _e : pr.getP1().getQ().outgoingEdgesOf(_v)) {
							_s.append("\t->").append(pr.getP1().getQ().getEdgeTarget(_e).nodeID);
						}
						writer.println(_s);
					}

					writer.println("......................");
					writer.println("P2_ID: " + pr.getP2().getPatternID());
					for (SimpleNode _v : pr.getP2().getQ().vertexSet()) {
						StringBuffer _s = new StringBuffer();
						_s.append(_v.nodeID).append("\t").append(_v.attribute);
						for (DefaultEdge _e : pr.getP2().getQ().outgoingEdgesOf(_v)) {
							_s.append("\t->").append(pr.getP2().getQ().getEdgeTarget(_e).nodeID);
						}
						writer.println(_s);
					}

					writer.println("----------------------");
				}
			}
			writer.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
