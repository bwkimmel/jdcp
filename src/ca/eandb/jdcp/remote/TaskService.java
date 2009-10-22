package ca.eandb.jdcp.remote;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.BitSet;
import java.util.UUID;

import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.util.rmi.Serialized;

/**
 * A remote service used by workers to perform tasks for
 * <code>ParallelizableJob</code>s.
 * @author Brad Kimmel
 */
public interface TaskService extends Remote {

	/**
	 * Gets the task worker for a job.
	 * @param jobId The <code>UUID</code> of the job to obtain the task worker
	 * 		for.
	 * @return The <code>TaskWorker</code> to use to process tasks for the job
	 * 		with the specified <code>UUID</code>, or <code>null</code> if that
	 * 		job is no longer available.
	 * @throws DelegationException If this <code>JobService</code> could not
	 * 		communicate with the provider of the <code>TaskWorker</code> being
	 * 		requested.
	 * @throws IllegalArgumentException If there is no job on the server with
	 * 		the specified <code>UUID</code>.
	 * @throws SecurityException If the caller does not have permission to
	 * 		obtain the task worker.
	 * @throws RemoteException If a communication error occurs.
	 */
	Serialized<TaskWorker> getTaskWorker(UUID jobId)
			throws DelegationException, IllegalArgumentException,
			SecurityException, RemoteException;

	/**
	 * Gets a task to perform.
	 * @return A <code>TaskDescription</code> describing the task to be
	 * 		performed.
	 * @throws SecurityException If the caller does not have permission to
	 * 		request tasks.
	 * @throws RemoteException If a communication error occurs.
	 */
	TaskDescription requestTask() throws SecurityException, RemoteException;

	/**
	 * Submits the results of a task.
	 * @param jobId The <code>UUID</code> identifying the job for which the
	 * 		task was performed.
	 * @param taskId The ID of the task that was performed.
	 * @param results The results of the task.
	 * @throws SecurityException If the caller does not have permission to
	 * 		submit task results.
	 * @throws RemoteException If a communication error occurs.
	 */
	void submitTaskResults(UUID jobId, int taskId, Serialized<Object> results)
			throws SecurityException, RemoteException;

	/**
	 * Report that an exception was thrown during the execution of an assigned
	 * task.
	 * @param jobId The <code>UUID</code> identifying the job for which the
	 * 		task was being performed.
	 * @param taskId The ID of the task that was being performed, or zero (0)
	 * 		if the exception was not thrown during the processing of a task
	 * 		(for example, when deserializing the TaskWorker).
	 * @param e The <code>Exception</code> raised by the task.
	 * @throws SecurityException If the caller does not have permission to call
	 * 		this method.
	 * @throws RemoteException If a problem with the connection is encountered.
	 */
	void reportException(UUID jobId, int taskId, Exception e)
			throws SecurityException, RemoteException;

	/**
	 * Determines if any of the specified tasks are no longer outstanding.
	 * @param jobIds An array of <code>UUID</code> indicating the IDs of the
	 * 		jobs corresponding to the tasks to check.
	 * @param taskIds An array indicating the IDs of the tasks to check (must
	 * 		be the same length as <code>jobIds</code>).
	 * @return A <code>BitSet</code> indicating which tasks are no longer
	 * 		outstanding.  If either <code>jobIds</code> or <code>taskIds</code>
	 * 		is null, an empty <code>BitSet</code> is returned.
	 * @throws IllegalArgumentException If
	 * 		<code>jobIds.length != taskIds.length</code>.
	 * @throws SecurityException If the caller does not have permission to get
	 * 		the completion status of tasks.
	 * @throws RemoteException If a communication error occurs.
	 */
	BitSet getFinishedTasks(UUID[] jobIds, int[] taskIds)
			throws IllegalArgumentException, SecurityException, RemoteException;

	/**
	 * Gets the MD5 digest for the definition of the given class associated
	 * with the specified job.
	 * @param name The fully qualified name of the class whose digest to
	 * 		obtain.
	 * @param jobId The <code>UUID</code> identifying the job for which to
	 * 		get the class digest.
	 * @throws DelegationException If this <code>JobService</code> could not
	 * 		communicate with the provider of the class whose digest is being
	 * 		requested.
	 * @throws SecurityException If the caller does not have permission to
	 * 		obtain class digests.
	 * @throws RemoteException If a communication error occurs.
	 */
	byte[] getClassDigest(String name, UUID jobId) throws DelegationException,
			SecurityException, RemoteException;

	/**
	 * Gets the definition of the given class associated with the specified
	 * job.
	 * @param name The fully qualified name of the class whose definition to
	 * 		obtain.
	 * @param jobId The <code>UUID</code> identifying the job for which to
	 * 		get the class definition.
	 * @throws DelegationException If this <code>JobService</code> could not
	 * 		communicate with the provider of the class whose definition is
	 * 		being requested.
	 * @throws SecurityException If the caller does not have permission to
	 * 		obtain class definitions.
	 * @throws RemoteException If a communication error occurs.
	 */
	byte[] getClassDefinition(String name, UUID jobId)
			throws DelegationException, SecurityException, RemoteException;

}