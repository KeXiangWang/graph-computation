package ed.inf.grape.communicate;

import java.rmi.Remote;
import java.rmi.RemoteException;

import ed.inf.grape.client.Command;

/**
 * Defines the interface through which the application programmer communicates
 * with the Master.
 * 
 * @author yecol
 */

public interface Client2Coordinator extends Remote {

	public void putTask(String query) throws RemoteException;

	public void preProcess() throws RemoteException;

	public void postProcess() throws RemoteException;
}
