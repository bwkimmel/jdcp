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

import java.io.EOFException;
import java.rmi.ConnectException;
import java.rmi.ConnectIOException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.UnknownHostException;
import java.rmi.UnmarshalException;
import java.util.BitSet;
import java.util.Date;
import java.util.UUID;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.DelegationException;
import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.util.rmi.Serialized;

/**
 * @author Brad
 *
 */
final class ServiceWrapper implements TaskService {

  private static final Logger logger = Logger.getLogger(ServiceWrapper.class);

  private final TaskService service;

  private Date idleUntil = new Date(0);

  public ServiceWrapper(TaskService service) {
    this.service = service;
  }

  public void shutdown() {
    /* nothing to do. */
  }

  private interface ServiceOperation<T> {
    T run(TaskService service) throws Exception;
  };

  private <T> T run(ServiceOperation<T> operation) throws DelegationException {
    TaskService service = this.service;
    try {
      if (logger.isInfoEnabled()) {
        logger.info(String.format("Running operation: %s", operation));
      }
      return operation.run(service);
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
        logger.error("Communication error", e);
        throw new DelegationException("Error occurred delegating to server", e);
      }
    } catch (Exception e) {
      logger.error("Communication error", e);
      throw new DelegationException("Error occurred delegating to server", e);
    }
    throw new DelegationException("No connection to server");
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.remote.TaskService#getClassDefinition(java.lang.String, java.util.UUID)
   */
  public byte[] getClassDefinition(final String name, final UUID jobId)
      throws DelegationException {
    return run(new ServiceOperation<byte[]>() {
      public byte[] run(TaskService service) throws RemoteException,
          SecurityException {
        return service.getClassDefinition(name, jobId);
      }
    });
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.remote.TaskService#getClassDigest(java.lang.String, java.util.UUID)
   */
  public byte[] getClassDigest(final String name, final UUID jobId)
      throws DelegationException {
    return run(new ServiceOperation<byte[]>() {
      public byte[] run(TaskService service) throws RemoteException,
          SecurityException {
        return service.getClassDigest(name, jobId);
      }
    });
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.remote.TaskService#getFinishedTasks(java.util.UUID[], int[])
   */
  public BitSet getFinishedTasks(final UUID[] jobIds, final int[] taskIds)
      throws DelegationException {
    return run(new ServiceOperation<BitSet>() {
      public BitSet run(TaskService service) throws RemoteException,
          SecurityException {
        return service.getFinishedTasks(jobIds, taskIds);
      }
    });
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.remote.TaskService#getTaskWorker(java.util.UUID)
   */
  public Serialized<TaskWorker> getTaskWorker(final UUID jobId)
      throws DelegationException {
    return run(new ServiceOperation<Serialized<TaskWorker>>() {
      public Serialized<TaskWorker> run(TaskService service) throws RemoteException,
          SecurityException {
        return service.getTaskWorker(jobId);
      }
    });
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.remote.TaskService#reportException(java.util.UUID, int, java.lang.Exception)
   */
  public void reportException(final UUID jobId, final int taskId, final Exception e)
      throws DelegationException {
    run(new ServiceOperation<Object>() {
      public Object run(TaskService service) throws RemoteException,
          SecurityException {
        service.reportException(jobId, taskId, e);
        return null;
      }
    });
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.remote.TaskService#requestTask()
   */
  public TaskDescription requestTask() throws DelegationException {
    return run(new ServiceOperation<TaskDescription>() {
      public TaskDescription run(TaskService service) throws RemoteException,
          SecurityException {
        return service.requestTask();
      }
    });
  }

  /* (non-Javadoc)
   * @see ca.eandb.jdcp.remote.TaskService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Serialized)
   */
  public void submitTaskResults(final UUID jobId, final int taskId,
      final Serialized<Object> results) throws DelegationException {
    run(new ServiceOperation<Object>() {
      public Object run(TaskService service) throws RemoteException,
          SecurityException {
        service.submitTaskResults(jobId, taskId, results);
        return null;
      }
    });
  }

}
