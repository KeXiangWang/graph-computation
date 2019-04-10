package ed.inf.grape.communicate;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

import ed.inf.grape.core.Worker;

/**
 * Represents a medium through which workers communicate with other workers
 * 
 * @author Yecol
 */
public class Worker2WorkerProxy implements Worker2Worker, Remote {
	/** The workerID to Worker map. **/
	private Map<String, Worker> mapWorkerIdToWorker;

	/**
	 * Constructs the worker2worker proxy
	 * 
	 * @param mapWorkerIdToWorker
	 *            Represents the WorkerID to worker map
	 */
	public Worker2WorkerProxy(Map<String, Worker> mapWorkerIdToWorker)
			throws RemoteException {
		this.mapWorkerIdToWorker = mapWorkerIdToWorker;
	}

	/**
	 * Method to send message to another worker
	 * 
	 * @param receiverWorkerID
	 *            the receiver worker
	 * @param outgoingMessages
	 *            set of messages to be sent to the worker
	 */
//	@Override
//	public void sendMessage(String receiverWorkerID,
//			List<Message> outgoingMessages) throws RemoteException {
//		mapWorkerIdToWorker.get(receiverWorkerID).receiveMessage(
//				outgoingMessages);
//
//	}
}