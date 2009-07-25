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
import java.sql.SQLException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.Map.Entry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.concurrent.BackgroundThreadFactory;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
public final class JobHub implements JobService {

	private static final Logger logger = Logger.getLogger(JobHub.class);

	private static final int DEFAULT_IDLE_SECONDS = 10;

	private static final long POLLING_INTERVAL = 10;

	private static final TimeUnit POLLING_UNITS = TimeUnit.SECONDS;

	private TaskDescription idleTask = new TaskDescription(null, 0, DEFAULT_IDLE_SECONDS);

	private final Queue<ServiceInfo> services = new LinkedList<ServiceInfo>();

	private final Map<UUID, ServiceInfo> routes = new WeakHashMap<UUID, ServiceInfo>();

	private final Map<String, ServiceInfo> hosts = new HashMap<String, ServiceInfo>();

	private final ScheduledExecutorService activeTaskPoller = Executors.newScheduledThreadPool(1, new BackgroundThreadFactory());

	private final Executor executor = Executors.newCachedThreadPool(new BackgroundThreadFactory());

	private final DataSource dataSource;

	public JobHub(DataSource dataSource) {
		this.dataSource = dataSource;
		Runnable poller = new Runnable() {
			public void run() {
				pollActiveTasks();
			}
		};
		activeTaskPoller.scheduleAtFixedRate(poller, POLLING_INTERVAL,
				POLLING_INTERVAL, POLLING_UNITS);
	}

	public static void prepareDataSource(DataSource ds) throws SQLException {
		ServiceInfo.prepareDataSource(ds);
	}

	private void pollActiveTasks() {
		for (ServiceInfo info : hosts.values()) {
			info.pollActiveTasks();
		}
	}

	public void shutdown() {
		activeTaskPoller.shutdown();
	}

	public synchronized void connect(final String hostname, final String username, final String password) {
		if (hosts.containsKey(hostname)) {
			disconnect(hostname);
		}
		ServiceInfo info = new ServiceInfo(hostname, username, password,
				dataSource);
		hosts.put(hostname, info);
		services.add(info);
	}

	public synchronized void disconnect(String hostname) {
		ServiceInfo info = hosts.get(hostname);
		if (info != null) {
			hosts.remove(hostname);
			services.remove(info);
			for (Entry<UUID, ServiceInfo> entry : routes.entrySet()) {
				if (entry.getValue() == info) {
					routes.remove(entry.getKey());
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDefinition(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDefinition(String name, UUID jobId) {
		ServiceInfo info = routes.get(jobId);
		if (info == null) {
			throw new IllegalArgumentException("No route for specified job ID");
		}
		return info.getClassDefinition(name, jobId);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDigest(String name, UUID jobId) {
		ServiceInfo info = routes.get(jobId);
		if (info == null) {
			throw new IllegalArgumentException("No route for specified job ID");
		}
		return info.getClassDigest(name, jobId);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getFinishedTasks(java.util.UUID[], int[])
	 */
	public BitSet getFinishedTasks(UUID[] jobIds, int[] taskIds)
			throws IllegalArgumentException {
		if (jobIds == null || taskIds == null) {
			return null;
		}
		if (jobIds.length != taskIds.length) {
			throw new IllegalArgumentException("jobIds.length != taskIds.length");
		}
		BitSet finished = new BitSet(jobIds.length);
		for (int i = 0; i < jobIds.length; i++) {
			ServiceInfo info = routes.get(jobIds[i]);
			finished.set(i, (info == null) || info.isTaskComplete(jobIds[i], taskIds[i]));
		}
		return finished;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getTaskWorker(java.util.UUID)
	 */
	public Serialized<TaskWorker> getTaskWorker(UUID jobId)
			throws IllegalArgumentException {
		ServiceInfo info = routes.get(jobId);
		if (info == null) {
			throw new IllegalArgumentException("No route for specified job id");
		}

		return info.getTaskWorker(jobId);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#reportException(java.util.UUID, int, java.lang.Exception)
	 */
	public void reportException(final UUID jobId, final int taskId,
			final Exception e) {
		final ServiceInfo info = routes.get(jobId);
		if (info != null) {
			executor.execute(new Runnable() {
				public void run() {
					try {
						info.reportException(jobId, taskId, e);
					} catch (Exception e1) {
						logger.error("Cannot report exception", e1);
					}
				}
			});
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#requestTask()
	 */
	public TaskDescription requestTask() {
		int n = services.size();
		if (n > 0) {
			ServiceInfo[] serv;
			synchronized (this) {
				serv = (ServiceInfo[]) services.toArray(new ServiceInfo[n]);
			}
			for (ServiceInfo info : serv) {
				try {
					synchronized (this) {
						services.remove(info);
						services.add(info);
					}
					TaskDescription task = info.requestTask();
					if (task != null) {
						UUID jobId = task.getJobId();
						routes.put(jobId, info);
						return task;
					}
				} catch (Exception e) {
					logger.error("Failed to request task from server", e);
				}
			}
		}
		return idleTask;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setIdleTime(int)
	 */
	public void setIdleTime(int idleSeconds) throws IllegalArgumentException {
		idleTask = new TaskDescription(null, 0, idleSeconds);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Serialized)
	 */
	public void submitTaskResults(final UUID jobId, final int taskId,
			final Serialized<Object> results) {
		final ServiceInfo info = routes.get(jobId);
		if (info != null) {
			executor.execute(new Runnable() {
				public void run() {
					try {
						info.submitTaskResults(jobId, taskId, results);
					} catch (Exception e) {
						logger.error("Cannot submit task results", e);
					}
				}
			});
		}
	}

	///////////////////////////////////////////////////////////////////////////
	// The following operations are not supported

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
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name) throws SecurityException,
			RemoteException {
		throw new UnsupportedOperationException();
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

}
