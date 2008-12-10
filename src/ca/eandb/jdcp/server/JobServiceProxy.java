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

package ca.eandb.jdcp.server;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.UUID;

import javax.security.auth.Subject;


import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.security.JdcpPermission;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad Kimmel
 *
 */
public final class JobServiceProxy extends UnicastRemoteObject implements JobService {

	/**
	 *
	 */
	private static final long serialVersionUID = -3663995122172056330L;

	private final Subject user;
	private final JobService service;

	/**
	 * @param user
	 * @param server
	 */
	public JobServiceProxy(Subject user, JobService service) throws RemoteException {
		this.user = user;
		this.service = service;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#cancelJob(java.util.UUID)
	 */
	@Override
	public void cancelJob(final UUID jobId) throws IllegalArgumentException,
			SecurityException, RemoteException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("cancelJob"));
					service.cancelJob(jobId);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#createJob(java.lang.String)
	 */
	@Override
	public UUID createJob(final String description) throws SecurityException,
			RemoteException {

		try {
			return Subject.doAsPrivileged(user, new PrivilegedExceptionAction<UUID>() {

				@Override
				public UUID run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("createJob"));
					return service.createJob(description);
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDefinition(java.lang.String, java.util.UUID)
	 */
	@Override
	public byte[] getClassDefinition(final String name, final UUID jobId)
			throws SecurityException, RemoteException {

		try {
			return Subject.doAsPrivileged(user, new PrivilegedExceptionAction<byte[]>() {

				@Override
				public byte[] run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("getJobClassDefinition"));
					return service.getClassDefinition(name, jobId);
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String, java.util.UUID)
	 */
	@Override
	public byte[] getClassDigest(final String name, final UUID jobId)
			throws SecurityException, RemoteException {

		try {
			return Subject.doAsPrivileged(user, new PrivilegedExceptionAction<byte[]>() {

				@Override
				public byte[] run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("getJobClassDigest"));
					return service.getClassDigest(name, jobId);
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String)
	 */
	@Override
	public byte[] getClassDigest(final String name) throws SecurityException,
			RemoteException {

		try {
			return Subject.doAsPrivileged(user, new PrivilegedExceptionAction<byte[]>() {

				@Override
				public byte[] run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("getGlobalClassDigest"));
					return service.getClassDigest(name);
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getTaskWorker(java.util.UUID)
	 */
	@Override
	public Serialized<TaskWorker> getTaskWorker(final UUID jobId)
			throws IllegalArgumentException, SecurityException, RemoteException {

		try {
			return Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Serialized<TaskWorker>>() {

				@Override
				public Serialized<TaskWorker> run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("getTaskWorker"));
					return service.getTaskWorker(jobId);
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#requestTask()
	 */
	@Override
	public TaskDescription requestTask() throws SecurityException,
			RemoteException {

		try {
			return Subject.doAsPrivileged(user, new PrivilegedExceptionAction<TaskDescription>() {

				@Override
				public TaskDescription run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("requestTask"));
					return service.requestTask();
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, byte[])
	 */
	@Override
	public void setClassDefinition(final String name, final byte[] def)
			throws SecurityException, RemoteException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("setGlobalClassDefinition"));
					service.setClassDefinition(name, def);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, java.util.UUID, byte[])
	 */
	@Override
	public void setClassDefinition(final String name, final UUID jobId, final byte[] def)
			throws IllegalArgumentException, SecurityException, RemoteException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("setJobClassDefinition"));
					service.setClassDefinition(name, jobId, def);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setIdleTime(int)
	 */
	@Override
	public void setIdleTime(final int idleSeconds) throws IllegalArgumentException,
			SecurityException, RemoteException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("setIdleTime"));
					service.setIdleTime(idleSeconds);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setJobPriority(java.util.UUID, int)
	 */
	@Override
	public void setJobPriority(final UUID jobId, final int priority)
			throws IllegalArgumentException, SecurityException, RemoteException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("setJobPriority"));
					service.setJobPriority(jobId, priority);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.util.UUID)
	 */
	@Override
	public void submitJob(final Serialized<ParallelizableJob> job, final UUID jobId)
			throws IllegalArgumentException, SecurityException,
			ClassNotFoundException, RemoteException, JobExecutionException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("submitJob"));
					service.submitJob(job, jobId);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof ClassNotFoundException) {
				throw (ClassNotFoundException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else if (e.getException() instanceof JobExecutionException) {
				throw (JobExecutionException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Serialized, java.lang.String)
	 */
	@Override
	public UUID submitJob(final Serialized<ParallelizableJob> job, final String description)
			throws SecurityException, ClassNotFoundException, RemoteException, JobExecutionException {

		try {
			return Subject.doAsPrivileged(user, new PrivilegedExceptionAction<UUID>() {

				@Override
				public UUID run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("submitJob"));
					return service.submitJob(job, description);
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof ClassNotFoundException) {
				throw (ClassNotFoundException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else if (e.getException() instanceof JobExecutionException) {
				throw (JobExecutionException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Serialized)
	 */
	@Override
	public void submitTaskResults(final UUID jobId, final int taskId,
			final Serialized<Object> results) throws SecurityException,
			RemoteException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("submitTaskResults"));
					service.submitTaskResults(jobId, taskId, results);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof IllegalArgumentException) {
				throw (IllegalArgumentException) e.getException();
			} else if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#reportException(java.util.UUID, int, java.lang.Exception)
	 */
	@Override
	public void reportException(final UUID jobId, final int taskId,
			final Exception ex) throws SecurityException,
			RemoteException {

		try {
			Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

				@Override
				public Object run() throws Exception {
					AccessController.checkPermission(new JdcpPermission("reportException"));
					service.reportException(jobId, taskId, ex);
					return null;
				}

			}, null);
		} catch (PrivilegedActionException e) {
			if (e.getException() instanceof SecurityException) {
				throw (SecurityException) e.getException();
			} else if (e.getException() instanceof RemoteException) {
				throw (RemoteException) e.getException();
			} else {
				throw new UnexpectedException(e);
			}
		}

	}

}
