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

package ca.eandb.jdcp.hub;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
final class ServiceWrapper implements JobService {

	private static final Logger logger = Logger.getLogger(ServiceWrapper.class);

	private static final int RECONNECT_INTERVAL = 60;

	private final String host;

	private final String username;

	private final String password;

	private JobService service = null;

	private Date idleUntil = new Date(0);

	public ServiceWrapper(String host, String username, String password) {
		this.host = host;
		this.username = username;
		this.password = password;
	}

	private interface ServiceOperation<T> {
		T run(JobService service) throws RemoteException, SecurityException;
	};

	private <T> T run(ServiceOperation<T> operation) throws RemoteException {
		if (service == null) {
			service = connect(host, username, password);
		}
		try {
			return operation.run(service);
		} catch (RemoteException e) {
			service = connect(host, username, password);
			return operation.run(service);
		}
	}

	private void idle() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.SECOND, RECONNECT_INTERVAL);
		idleUntil = cal.getTime();
	}

	private synchronized JobService connect(String host, String username, String password) {
		Date now = new Date();
		if (now.after(idleUntil)) {
			try {
				Registry registry = LocateRegistry.getRegistry(host, 5327);
				AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
				return auth.authenticate(username, password);
			} catch (Exception e) {
				logger.error("Job service not found at remote host.", e);
				idle();
				throw new RuntimeException("Could not connect to remote host", e);
			}
		} else {
			throw new RuntimeException("Connection to remote host is down.");
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#cancelJob(java.util.UUID)
	 */
	public void cancelJob(UUID jobId) throws IllegalArgumentException,
			SecurityException, RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#createJob(java.lang.String)
	 */
	public UUID createJob(String description) throws SecurityException,
			RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDefinition(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDefinition(final String name, final UUID jobId)
			throws SecurityException, RemoteException {
		return run(new ServiceOperation<byte[]>() {
			public byte[] run(JobService service) throws RemoteException,
					SecurityException {
				return service.getClassDefinition(name, jobId);
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDigest(final String name, final UUID jobId)
			throws SecurityException, RemoteException {
		return run(new ServiceOperation<byte[]>() {
			public byte[] run(JobService service) throws RemoteException,
					SecurityException {
				return service.getClassDigest(name, jobId);
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name) throws SecurityException,
			RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getFinishedTasks(java.util.UUID[], int[])
	 */
	public BitSet getFinishedTasks(final UUID[] jobIds, final int[] taskIds)
			throws IllegalArgumentException, SecurityException, RemoteException {
		return run(new ServiceOperation<BitSet>() {
			public BitSet run(JobService service) throws RemoteException,
					SecurityException {
				return service.getFinishedTasks(jobIds, taskIds);
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getTaskWorker(java.util.UUID)
	 */
	public Serialized<TaskWorker> getTaskWorker(final UUID jobId)
			throws IllegalArgumentException, SecurityException, RemoteException {
		return run(new ServiceOperation<Serialized<TaskWorker>>() {
			public Serialized<TaskWorker> run(JobService service) throws RemoteException,
					SecurityException {
				return service.getTaskWorker(jobId);
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#reportException(java.util.UUID, int, java.lang.Exception)
	 */
	public void reportException(final UUID jobId, final int taskId, final Exception e)
			throws SecurityException, RemoteException {
		run(new ServiceOperation<Object>() {
			public Object run(JobService service) throws RemoteException,
					SecurityException {
				service.reportException(jobId, taskId, e);
				return null;
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#requestTask()
	 */
	public TaskDescription requestTask() throws SecurityException,
			RemoteException {
		return run(new ServiceOperation<TaskDescription>() {
			public TaskDescription run(JobService service) throws RemoteException,
					SecurityException {
				return service.requestTask();
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, byte[])
	 */
	public void setClassDefinition(String name, byte[] def)
			throws SecurityException, RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, java.util.UUID, byte[])
	 */
	public void setClassDefinition(String name, UUID jobId, byte[] def)
			throws IllegalArgumentException, SecurityException, RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setIdleTime(int)
	 */
	public void setIdleTime(int idleSeconds) throws IllegalArgumentException,
			SecurityException, RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setJobPriority(java.util.UUID, int)
	 */
	public void setJobPriority(UUID jobId, int priority)
			throws IllegalArgumentException, SecurityException, RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.util.UUID)
	 */
	public void submitJob(Serialized<ParallelizableJob> job, UUID jobId)
			throws IllegalArgumentException, SecurityException,
			ClassNotFoundException, RemoteException, JobExecutionException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.lang.String)
	 */
	public UUID submitJob(Serialized<ParallelizableJob> job, String description)
			throws SecurityException, ClassNotFoundException, RemoteException,
			JobExecutionException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Serialized)
	 */
	public void submitTaskResults(final UUID jobId, final int taskId,
			final Serialized<Object> results) throws SecurityException,
			RemoteException {
		run(new ServiceOperation<Object>() {
			public Object run(JobService service) throws RemoteException,
					SecurityException {
				service.submitTaskResults(jobId, taskId, results);
				return null;
			}
		});
	}

}
