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

package ca.eandb.jdcp.worker;

import java.io.EOFException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.UnknownHostException;
import java.rmi.UnmarshalException;
import java.util.BitSet;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.util.rmi.Serialized;

/**
 * A <code>JobService</code> wrapper that automatically reconnects when the
 * connection is lost.
 * @author Brad Kimmel
 */
public final class ReconnectingJobService implements JobService {

	private static final Logger logger = Logger.getLogger(ReconnectingJobService.class);

	private final JobServiceFactory factory;

	private JobService service;


	public ReconnectingJobService(JobServiceFactory factory) {
		this.factory = factory;
	}

	private synchronized JobService getJobService(JobService oldService) {
		if (service == oldService) {
			service = factory.connect();
		}
		return service;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#cancelJob(java.util.UUID)
	 */
	public void cancelJob(UUID jobId) throws IllegalArgumentException,
			SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.cancelJob(jobId);
				return;
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#createJob(java.lang.String)
	 */
	public UUID createJob(String description) throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.createJob(description);
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDefinition(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDefinition(String name, UUID jobId)
			throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.getClassDefinition(name, jobId);
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDigest(String name, UUID jobId)
			throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.getClassDigest(name, jobId);
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name) throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.getClassDigest(name);
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getTaskWorker(java.util.UUID)
	 */
	public Serialized<TaskWorker> getTaskWorker(UUID jobId)
			throws IllegalArgumentException, SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.getTaskWorker(jobId);
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#reportException(java.util.UUID, int, java.lang.Exception)
	 */
	public void reportException(UUID jobId, int taskId, Exception e)
			throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.reportException(jobId, taskId, e);
				return;
			} catch (RemoteException e1) {
				logger.error("Lost connection", e1);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#requestTask()
	 */
	public TaskDescription requestTask() throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.requestTask();
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, byte[])
	 */
	public void setClassDefinition(String name, byte[] def)
			throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.setClassDefinition(name, def);
				return;
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, java.util.UUID, byte[])
	 */
	public void setClassDefinition(String name, UUID jobId, byte[] def)
			throws IllegalArgumentException, SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.setClassDefinition(name, jobId, def);
				return;
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setIdleTime(int)
	 */
	public void setIdleTime(int idleSeconds) throws IllegalArgumentException,
			SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.setIdleTime(idleSeconds);
				return;
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setJobPriority(java.util.UUID, int)
	 */
	public void setJobPriority(UUID jobId, int priority)
			throws IllegalArgumentException, SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.setJobPriority(jobId, priority);
				return;
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.util.UUID)
	 */
	public void submitJob(Serialized<ParallelizableJob> job, UUID jobId)
			throws IllegalArgumentException, SecurityException,
			ClassNotFoundException, JobExecutionException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.submitJob(job, jobId);
				return;
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.lang.String)
	 */
	public UUID submitJob(Serialized<ParallelizableJob> job, String description)
			throws SecurityException, ClassNotFoundException,
			JobExecutionException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.submitJob(job, description);
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Serialized)
	 */
	public void submitTaskResults(UUID jobId, int taskId,
			Serialized<Object> results) throws SecurityException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.submitTaskResults(jobId, taskId, results);
				return;
			} catch (RemoteException e) {
				logger.error("Lost connection", e);
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getFinishedTasks(java.util.UUID[], int[])
	 */
	public BitSet getFinishedTasks(UUID[] jobIds, int[] taskIds)
			throws IllegalArgumentException, SecurityException, RemoteException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				return service.getFinishedTasks(jobIds, taskIds);
			} catch (NoSuchObjectException e) {
				logger.error("Lost connection", e);
			} catch (ConnectException e) {
				logger.error("Lost connection", e);
			} catch (ConnectIOException e) {
				logger.error("Lost connection", e);
			} catch (UnknownHostException e) {
				logger.error("Lost connection", e);
			} catch (UnmarshalException e) {
				if (e.getCause() instanceof EOFException) {
					logger.error("Lost connection", e);
				} else {
					throw e;
				}
			}
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#registerTaskService(java.lang.String, ca.eandb.jdcp.remote.TaskService)
	 */
	public void registerTaskService(String name, TaskService taskService)
			throws SecurityException, RemoteException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.registerTaskService(name, taskService);
				return;
			} catch (NoSuchObjectException e) {
				logger.error("Lost connection", e);
			} catch (ConnectException e) {
				logger.error("Lost connection", e);
			} catch (ConnectIOException e) {
				logger.error("Lost connection", e);
			} catch (UnknownHostException e) {
				logger.error("Lost connection", e);
			} catch (UnmarshalException e) {
				if (e.getCause() instanceof EOFException) {
					logger.error("Lost connection", e);
				} else {
					throw e;
				}
			}
		}	
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#unregisterTaskService(java.lang.String)
	 */
	public void unregisterTaskService(String name)
			throws IllegalArgumentException, SecurityException, RemoteException {
		JobService service = null;
		while (true) {
			try {
				service = getJobService(service);
				service.unregisterTaskService(name);
				return;
			} catch (NoSuchObjectException e) {
				logger.error("Lost connection", e);
			} catch (ConnectException e) {
				logger.error("Lost connection", e);
			} catch (ConnectIOException e) {
				logger.error("Lost connection", e);
			} catch (UnknownHostException e) {
				logger.error("Lost connection", e);
			} catch (UnmarshalException e) {
				if (e.getCause() instanceof EOFException) {
					logger.error("Lost connection", e);
				} else {
					throw e;
				}
			}
		}
	}

}
