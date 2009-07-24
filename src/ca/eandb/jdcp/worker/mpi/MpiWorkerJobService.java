/*
 * Copyright (c) 2008 Bradley W. Kimmel
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package ca.eandb.jdcp.worker.mpi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.rmi.RemoteException;
import java.util.BitSet;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
public final class MpiWorkerJobService implements JobService {

	private static final Logger logger = Logger.getLogger(MpiWorkerJobService.class);

	private static final int GET_CLASS_DEFINITION = 1;
	private static final int GET_CLASS_DIGEST = 2;
	private static final int GET_FINISHED_TASKS = 3;
	private static final int GET_TASK_WORKER = 4;
	private static final int REPORT_EXCEPTION = 5;
	private static final int REQUEST_TASK = 6;
	private static final int SUBMIT_TASK_RESULTS = 7;

	private static synchronized native byte[] mpiCall(int methodId, byte[] payload);

	private static Object call(int methodId, Object... params) throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		if (params != null) {
			for (int i = 0; i < params.length; i++) {
				oos.writeObject(params[i]);
			}
			oos.flush();
		}

		byte[] result = mpiCall(methodId, bos.toByteArray());
		ByteArrayInputStream bis = new ByteArrayInputStream(result);
		ObjectInputStream ois = new ObjectInputStream(bis);
		boolean success = ois.readBoolean();
		Object value = ois.readObject();
		if (success) {
			return value;
		} else {
			throw (Exception) value;
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#cancelJob(java.util.UUID)
	 */
	public void cancelJob(UUID jobId)  {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#createJob(java.lang.String)
	 */
	public UUID createJob(String description) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDefinition(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDefinition(String name, UUID jobId)
			throws SecurityException, RemoteException {
		try {
			logger.info("Calling GET_CLASS_DEFINITION");
			return (byte[]) call(GET_CLASS_DEFINITION, name, jobId);
		} catch (SecurityException e) {
			throw e;
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new UnexpectedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDigest(String name, UUID jobId)
			throws SecurityException, RemoteException {
		try {
			logger.info("Calling GET_CLASS_DIGEST");
			return (byte[]) call(GET_CLASS_DIGEST, name, jobId);
		} catch (SecurityException e) {
			throw e;
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new UnexpectedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name)  {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getFinishedTasks(java.util.UUID[], int[])
	 */
	public BitSet getFinishedTasks(UUID[] jobIds, int[] taskIds)
			throws IllegalArgumentException, SecurityException, RemoteException {
		try {
			logger.info("Calling GET_FINISHED_TASKS");
			return (BitSet) call(GET_FINISHED_TASKS, jobIds, taskIds);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (SecurityException e) {
			throw e;
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new UnexpectedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getTaskWorker(java.util.UUID)
	 */
	@SuppressWarnings("unchecked")
	public Serialized<TaskWorker> getTaskWorker(UUID jobId)
			throws IllegalArgumentException, SecurityException, RemoteException {
		try {
			logger.info("Calling GET_TASK_WORKER");
			return (Serialized<TaskWorker>) call(GET_TASK_WORKER, jobId);
		} catch (IllegalArgumentException e) {
			throw e;
		} catch (SecurityException e) {
			throw e;
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new UnexpectedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#reportException(java.util.UUID, int, java.lang.Exception)
	 */
	public void reportException(UUID jobId, int taskId, Exception e)
			throws SecurityException, RemoteException {
		try {
			logger.info("Calling REPORT_EXCEPTION");
			call(REPORT_EXCEPTION, jobId, taskId, e);
		} catch (SecurityException e1) {
			throw e1;
		} catch (RemoteException e1) {
			throw e1;
		} catch (Exception e1) {
			throw new UnexpectedException(e1);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#requestTask()
	 */
	public TaskDescription requestTask() throws SecurityException,
			RemoteException {
		try {
			logger.info("Calling REQUEST_TASK");
			return (TaskDescription) call(REQUEST_TASK);
		} catch (SecurityException e) {
			throw e;
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new UnexpectedException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, byte[])
	 */
	public void setClassDefinition(String name, byte[] def) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, java.util.UUID, byte[])
	 */
	public void setClassDefinition(String name, UUID jobId, byte[] def) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setIdleTime(int)
	 */
	public void setIdleTime(int idleSeconds) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setJobPriority(java.util.UUID, int)
	 */
	public void setJobPriority(UUID jobId, int priority) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.util.UUID)
	 */
	public void submitJob(Serialized<ParallelizableJob> job, UUID jobId) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.lang.String)
	 */
	public UUID submitJob(Serialized<ParallelizableJob> job, String description) {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Serialized)
	 */
	public void submitTaskResults(UUID jobId, int taskId,
			Serialized<Object> results) throws SecurityException,
			RemoteException {
		try {
			logger.info("Calling SUBMIT_TASK_RESULTS");
			call(SUBMIT_TASK_RESULTS, jobId, taskId, results);
		} catch (SecurityException e) {
			throw e;
		} catch (RemoteException e) {
			throw e;
		} catch (Exception e) {
			throw new UnexpectedException(e);
		}
	}

}
