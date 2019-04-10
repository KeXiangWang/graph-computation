package ed.inf.grape.communicate;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.List;

import ed.inf.discovery.Pattern;
import ed.inf.grape.core.Worker;

/**
 * Defines an interface to register remote Worker with the coordinator
 * 
 * @author Yecol
 */

public interface Worker2Coordinator extends java.rmi.Remote, Serializable {

	/**
	 * Registers remote workers with the master.
	 * 
	 * @param worker
	 *            the worker
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            the num worker threads
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */

	public Worker2Coordinator register(Worker worker, String workerID, int numWorkerThreads)
			throws RemoteException;

	/**
	 * Send a message to the Master saying that the current computation has been
	 * completed.
	 * 
	 * @param workerID
	 *            the worker id
	 */

	public void sendMessageWorker2Coordinator(String workerID, List<Pattern> messages)
			throws RemoteException;

//	public void sendMetaInfo2Coordinator(String workerID, int Ycount, int NotYCount)
//			throws RemoteException;

	public void sendMessageCoordinator2Worker(List<Pattern> messages) throws RemoteException;

	public void shutdown() throws RemoteException;

}
