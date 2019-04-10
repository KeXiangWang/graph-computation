package ed.inf.grape.communicate;


/**
 * Interface for the workers to communicate between them
 * 
 * @author Yecol
 */
public interface Worker2Worker extends java.rmi.Remote {
	/**
	 * Method to send message to another worker
	 * 
	 * @param receiverWorkerID
	 *            the receiver worker
	 * @param outgoingMessages
	 *            set of messages to be sent to the worker
	 */

//	public void sendMessage(String receiverWorkerID,
//			List<Message> outgoingMessages) throws RemoteException;
}
