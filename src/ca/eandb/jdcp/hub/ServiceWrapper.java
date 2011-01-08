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

import java.io.EOFException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.UnknownHostException;
import java.rmi.UnmarshalException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.BitSet;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.DelegationException;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
final class ServiceWrapper implements JobService {

	private static final Logger logger = Logger.getLogger(ServiceWrapper.class);

	private static final long RECONNECT_INTERVAL = 60000;

	private final String host;
	
	private final int port;

	private final String username;

	private final String password;

	private JobService service = null;

	private Date idleUntil = new Date(0);

	private final Thread keepAlive;

	private boolean shutdown = false;

	public ServiceWrapper(String host, int port, String username, String password) {
		this.host = host;
		this.port = port;
		this.username = username;
		this.password = password;

		this.keepAlive = new Thread(new Runnable() {
			public void run() {
				keepAlive();
			}
		});
		keepAlive.start();
	}

	public void shutdown() {
		synchronized (keepAlive) {
			shutdown = true;
			keepAlive.interrupt();
		}
	}

	private void keepAlive() {
		while (!shutdown) {
			while (service == null && !shutdown) {
				try {
					service = connect(host, port, username, password);
				} catch (Exception e) {
					logger.error("Could not connect to remote host", e);
				}
				if (service != null) {
					logger.info("Successfully connected to remote host");
					break;
				}
				logger.info("Could not connect, waiting...");
				try {
					Thread.sleep(RECONNECT_INTERVAL);
				} catch (InterruptedException e) {
					/* nothing to do. */
				}
			}
			logger.info("Connected, keepAlive going to sleep");
			try {
				synchronized (keepAlive) {
					keepAlive.wait();
				}
			} catch (InterruptedException e) {
				/* nothing to do. */
			}
		}
		logger.info("Shutting down keepAlive");
	}

	private interface ServiceOperation<T> {
		T run(JobService service) throws Exception;
	};

	private <T> T run(ServiceOperation<T> operation) throws DelegationException {
		JobService service = this.service;
		if (service != null) {
			try {
				if (logger.isInfoEnabled()) {
					logger.info(String.format("Running operation: %s", operation));
				}
				return operation.run(service);
			} catch (NoSuchObjectException e) {
				this.service = null;
				logger.error("Lost connection", e);
			} catch (ConnectException e) {
				this.service = null;
				logger.error("Lost connection", e);
			} catch (ConnectIOException e) {
				this.service = null;
				logger.error("Lost connection", e);
			} catch (UnknownHostException e) {
				this.service = null;
				logger.error("Lost connection", e);
			} catch (UnmarshalException e) {
				if (e.getCause() instanceof EOFException) {
					this.service = null;
					logger.error("Lost connection", e);
				} else {
					logger.error("Communication error", e);
					throw new DelegationException("Error occurred delegating to server", e);
				}
			} catch (Exception e) {
				logger.error("Communication error", e);
				throw new DelegationException("Error occurred delegating to server", e);
			}
		}
		logger.info("Signalling connection thread to reconnect");
		synchronized (keepAlive) {
			keepAlive.notify();
		}
		throw new DelegationException("No connection to server");
	}

	private synchronized JobService connect(String host, int port, String username,
			String password) throws DelegationException {
		if (logger.isInfoEnabled()) {
			logger.info(String.format("connect(host='%s', port=%d, username='%s', password='%s')", host, port, username, password));
		}
		Date now = new Date();
		if (now.after(idleUntil)) {
			try {
				logger.info("Locating registry");
				Registry registry = LocateRegistry.getRegistry(host, port);
				logger.info("Looking up AuthenticationService");
				AuthenticationService auth = (AuthenticationService) registry.lookup("AuthenticationService");
				logger.info("Authenticating");
				return auth.authenticate(username, password, JdcpUtil.PROTOCOL_VERSION_ID);
			} catch (Exception e) {
				logger.error("Job service not found at remote host.", e);
				throw new DelegationException("Could not connect to remote host", e);
			}
		} else {
			logger.info("Will not connect, idling");
			throw new DelegationException("Connection to remote host is down.");
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
			throws DelegationException {
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
			throws DelegationException {
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
			throws DelegationException {
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
			throws DelegationException {
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
			throws DelegationException {
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
	public TaskDescription requestTask() throws DelegationException {
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
			final Serialized<Object> results) throws DelegationException {
		run(new ServiceOperation<Object>() {
			public Object run(JobService service) throws RemoteException,
					SecurityException {
				service.submitTaskResults(jobId, taskId, results);
				return null;
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#registerTaskService(java.lang.String, ca.eandb.jdcp.remote.TaskService)
	 */
	public void registerTaskService(String name, TaskService service)
			throws SecurityException, RemoteException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#unregisterTaskService(java.lang.String)
	 */
	public void unregisterTaskService(String name) throws SecurityException,
			RemoteException {
		throw new UnsupportedOperationException();
	}

}
