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
import java.util.BitSet;
import java.util.UUID;

import javax.security.auth.Subject;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.JobStatus;
import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.jdcp.security.JdcpPermission;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.rmi.Serialized;

/**
 * A proxy <code>JobService</code> object used by clients of a remote
 * <code>JobService</code>.
 * @author Brad Kimmel
 */
public final class JobServiceProxy extends UnicastRemoteObject implements JobService {

  /**
   * Serialization version ID.
   */
  private static final long serialVersionUID = -3663995122172056330L;

  /** The <code>Subject</code> that the user has authenticated as. */
  private final Subject user;

  /** The underlying <code>JobService</code>. */
  private final JobService service;

  /**
   * Creates a new <code>JobServiceProxy</code>.
   * @param user The <code>Subject</code> that the user has authenticated as.
   * @param service The underlying <code>JobService</code>.
   * @throws RemoteException If a communication error occurs.
   */
  public JobServiceProxy(Subject user, JobService service) throws RemoteException {
    super(JdcpUtil.DEFAULT_PORT);
    this.user = user;
    this.service = service;
  }

  @Override
  public void cancelJob(final UUID jobId) throws IllegalArgumentException,
      SecurityException, RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public UUID createJob(final String description) throws SecurityException,
      RemoteException {

    try {
      return (UUID) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<UUID>() {

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

  @Override
  public byte[] getClassDefinition(final String name, final UUID jobId)
      throws SecurityException, RemoteException {

    try {
      return (byte[]) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<byte[]>() {

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

  @Override
  public byte[] getClassDigest(final String name, final UUID jobId)
      throws SecurityException, RemoteException {

    try {
      return (byte[]) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<byte[]>() {

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

  @Override
  public byte[] getClassDigest(final String name) throws SecurityException,
      RemoteException {

    try {
      return (byte[]) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<byte[]>() {

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

  @Override
  public Serialized<TaskWorker> getTaskWorker(final UUID jobId)
      throws IllegalArgumentException, SecurityException, RemoteException {

    try {
      return (Serialized<TaskWorker>) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Serialized<TaskWorker>>() {

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

  @Override
  public TaskDescription requestTask() throws SecurityException,
      RemoteException {

    try {
      return (TaskDescription) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<TaskDescription>() {

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

  @Override
  public void setClassDefinition(final String name, final byte[] def)
      throws SecurityException, RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public void setClassDefinition(final String name, final UUID jobId, final byte[] def)
      throws IllegalArgumentException, SecurityException, RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public void setIdleTime(final int idleSeconds) throws IllegalArgumentException,
      SecurityException, RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public void setJobPriority(final UUID jobId, final int priority)
      throws IllegalArgumentException, SecurityException, RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public void submitJob(final Serialized<ParallelizableJob> job, final UUID jobId)
      throws IllegalArgumentException, SecurityException,
      ClassNotFoundException, RemoteException, JobExecutionException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public UUID submitJob(final Serialized<ParallelizableJob> job, final String description)
      throws SecurityException, ClassNotFoundException, RemoteException, JobExecutionException {

    try {
      return (UUID) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<UUID>() {

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

  @Override
  public void submitTaskResults(final UUID jobId, final int taskId,
      final Serialized<Object> results) throws SecurityException,
      RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public void reportException(final UUID jobId, final int taskId,
      final Exception ex) throws SecurityException,
      RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

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

  @Override
  public BitSet getFinishedTasks(final UUID[] jobIds, final int[] taskIds)
      throws SecurityException, RemoteException {

    try {
      return (BitSet) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<BitSet>() {

        public BitSet run() throws Exception {
          AccessController.checkPermission(new JdcpPermission("getFinishedTasks"));
          return service.getFinishedTasks(jobIds, taskIds);
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

  @Override
  public void registerTaskService(final String name, final TaskService taskService)
      throws SecurityException, RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

        public Object run() throws Exception {
          AccessController.checkPermission(new JdcpPermission("registerTaskService"));
          service.registerTaskService(name, taskService);
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

  @Override
  public void unregisterTaskService(final String name)
      throws IllegalArgumentException, SecurityException, RemoteException {

    try {
      Subject.doAsPrivileged(user, new PrivilegedExceptionAction<Object>() {

        public Object run() throws Exception {
          AccessController.checkPermission(new JdcpPermission("unregisterTaskService"));
          service.unregisterTaskService(name);
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

  @Override
  public JobStatus waitForJobStatusChange(final long lastEventId, final long timeoutMillis)
      throws SecurityException, RemoteException {

    try {
      return (JobStatus) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<JobStatus>() {

        public JobStatus run() throws Exception {
          AccessController.checkPermission(new JdcpPermission("waitForJobStatusChange"));
          return service.waitForJobStatusChange(lastEventId, timeoutMillis);
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

  @Override
  public JobStatus waitForJobStatusChange(final UUID jobId, final long lastEventId,
      final long timeoutMillis) throws IllegalArgumentException,
      SecurityException, RemoteException {

    try {
      return (JobStatus) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<JobStatus>() {

        public JobStatus run() throws Exception {
          AccessController.checkPermission(new JdcpPermission("waitForJobStatusChange"));
          return service.waitForJobStatusChange(jobId, lastEventId, timeoutMillis);
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

  @Override
  public JobStatus getJobStatus(final UUID jobId) throws IllegalArgumentException,
      SecurityException, RemoteException {

    try {
      return (JobStatus) Subject.doAsPrivileged(user, new PrivilegedExceptionAction<JobStatus>() {

        public JobStatus run() throws Exception {
          AccessController.checkPermission(new JdcpPermission("getJobStatus"));
          return service.getJobStatus(jobId);
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

}
