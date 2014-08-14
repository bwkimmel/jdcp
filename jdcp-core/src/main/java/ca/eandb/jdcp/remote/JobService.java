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

package ca.eandb.jdcp.remote;

import java.rmi.RemoteException;
import java.util.UUID;

import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.util.rmi.Serialized;

/**
 * A remote service for accepting <code>ParallelizableJob</code>s,
 * managing the distribution of tasks to workers, and aggregating the results
 * submitted by workers.
 * @author Brad Kimmel
 */
public interface JobService extends TaskService {

  /** The default job priority. */
  public static final int DEFAULT_PRIORITY = 20;

  /* *********************************
   * Job submission/management methods
   */

  /**
   * Creates a new job on the server.  This method allows job-specified
   * classes to be uploaded before the job is submitted for execution.
   * @param description A description for the job.
   * @return The <code>UUID</code> identifying the new job.
   * @see #setClassDefinition(String, UUID, byte[])
   * @see #submitJob(Serialized, UUID)
   */
  UUID createJob(String description) throws SecurityException, RemoteException;

  /**
   * Submits a job previously created using {@link #createJob(String)}.
   * @param job The <code>ParallelizableJob</code> being submitted.  It must
   *     be wrapped in a <code>Serialized</code> wrapper to bypass RMI
   *     deserialization.
   * @param jobId The <code>UUID</code> associated with the previously
   *     created job.
   * @throws IllegalArgumentException If <code>jobId</code> does not
   *     correspond to a job on the server that is awaiting submission.
   * @throws SecurityException If the caller does not have permission to
   *     submit jobs.
   * @throws ClassNotFoundException If deserialization of <code>job</code>
   *     requires a class which does not exist on the server.
   * @throws RemoteException If a communication error occurs.
   * @throws JobExecutionException If the submitted job throws an exception
   *     while being initialized.
   */
  void submitJob(Serialized<ParallelizableJob> job, UUID jobId)
      throws IllegalArgumentException, SecurityException,
      ClassNotFoundException, RemoteException, JobExecutionException;

  /**
   * Submits a new job to be processed.
   * @param job The <code>ParallelizableJob</code> to be processed.
   * @param priority The priority to assign to the job.
   * @return The <code>UUID</code> assigned to the job, or <code>null</code>
   *     if the job was not accepted.
   * @throws SecurityException If the caller does not have permission to
   *     submit jobs.
   * @throws ClassNotFoundException If deserialization of <code>job</code>
   *     requires a class which does not exist on the server.
   * @throws RemoteException If a communication error occurs.
   * @throws JobExecutionException If the submitted job throws an exception
   *     while being initialized.
   */
  UUID submitJob(Serialized<ParallelizableJob> job, String description)
      throws SecurityException, ClassNotFoundException, RemoteException,
      JobExecutionException;

  /**
   * Cancels the specified job.
   * @param jobId The <code>UUID</code> identifying the job to be cancelled.
   * @throws IllegalArgumentException If there is no job on the server with
   *     the specified <code>UUID</code>.
   * @throws SecurityException If the caller does not have permission to
   *     cancel jobs.
   * @throws RemoteException If a communication error occurs.
   */
  void cancelJob(UUID jobId) throws IllegalArgumentException,
      SecurityException, RemoteException;

  /* ************************
   * Class management methods
   */

  /**
   * Gets the MD5 digest for the most recent definition of the given class.
   * @param name The fully qualified name of the class whose digest to
   *     obtain.
   * @param jobId The <code>UUID</code> identifying the job for which to
   *     get the class digest.
   * @throws DelegationException If this <code>JobService</code> could not
   *     communicate with the provider of the class whose digest is being
   *     requested.
   * @throws SecurityException If the caller does not have permission to
   *     obtain class digests.
   * @throws RemoteException If a communication error occurs.
   */
  byte[] getClassDigest(String name) throws SecurityException,
      RemoteException;

  /**
   * Sets the class definition for the specified class.
   * @param name The fully qualified name of the class whose definition is
   *     being provided.
   * @param def The definition of the class.
   * @throws SecurityException If the caller does not have permission to set
   *     class definitions.
   * @throws RemoteException If a communication error occurs.
   */
  void setClassDefinition(String name, byte[] def) throws SecurityException,
      RemoteException;

  /**
   * Sets the definition of a given class only for the specified job.
   * @param name The fully qualified name of the class whose definition is
   *     being provided.
   * @param jobId The <code>UUID</code> identifying the job to associate the
   *     class definition with.
   * @param def The definition of the class.
   * @throws IllegalArgumentException If there is no job awaiting submission
   *     with the specified job ID.
   * @throws SecurityException If the caller does not have permission to set
   *     class definitions.
   * @throws RemoteException If a communication error occurs.
   */
  void setClassDefinition(String name, UUID jobId, byte[] def)
      throws IllegalArgumentException, SecurityException, RemoteException;


  /* **********************
   * Administrative methods
   */

  /**
   * Sets the amount of time (in seconds) that workers should idle when there
   * are no tasks to be performed.
   * @param idleSeconds The amount of time (in seconds) that workers should
   *     idle when there are no tasks to be performed.
   * @throws IllegalArgumentException If <code>idleSeconds</code> is
   *     negative.
   * @throws SecurityException If the caller does not have permission to set
   *     the idle time.
   * @throws RemoteException If a communication error occurs.
   */
  void setIdleTime(int idleSeconds) throws IllegalArgumentException,
      SecurityException, RemoteException;

  /**
   * Sets the priority of the specified job.
   * @param jobId The <code>UUID</code> identifying the job whose priority is
   *     to be set.
   * @param priority The new priority to assign to the job.
   * @throws IllegalArgumentException If the priority is invalid or if there
   *     is no job with the specified job ID on the server.
   * @throws SecurityException If the caller does not have permission to set
   *     job priorities.
   * @throws RemoteException If a communication error occurs.
   */
  void setJobPriority(UUID jobId, int priority)
      throws IllegalArgumentException, SecurityException, RemoteException;

  /**
   * Registers a <code>TaskService</code> to receive tasks from an external
   * source.
   * @param name A unique string identifying the service to register.  If a
   *     service with the same name is already registered, it will be
   *     unregistered first.
   * @param service The <code>TaskService</code> to use to obtain tasks to
   *     work on.
   * @throws SecurityException If the caller does not have permission to
   *     register a <code>TaskService</code>.
   * @throws RemoteException If a communication error occurs.
   */
  void registerTaskService(String name, TaskService service) throws SecurityException,
      RemoteException;
  
  /**
   * Unregisters a <code>TaskService</code>.
   * @param name The name of identifying the <code>TaskService</code> to
   *     unregister.
   * @throws IllegalArgumentException If <code>serviceId</code> does not
   *     refer to a registered <code>TaskService</code>.
   * @throws SecurityException If the caller does not have permission to
   *     unregister a <code>TaskService</code>.
   * @throws RemoteException If a communication error occurs.
   */
  void unregisterTaskService(String name) throws IllegalArgumentException,
      SecurityException, RemoteException;

  
  /* ******************
   * Monitoring methods
   */
  
  /**
   * Waits for a status change for any job hosted on this server.
   * @param lastEventId The ID of the last event received, or
   *     <code>Long.MIN_VALUE</code> to indicate that no events had been
   *     received previously.
   * @param timeoutMillis The maximum amount of time (in milliseconds) to wait
   *     before returning.  If zero, then the call will return immediately.
   *     If negative, the call will wait indefinitely.
   * @return If an event has already occurred subsequent to the event with ID
   *     <code>lastEventId</code>, the pending <code>JobStatus</code> event
   *     will be returned.  Otherwise, the call will wait up to
   *     <code>timeoutMillis</code> milliseconds for an event to occur.  If
   *     one does occur in that time, that <code>JobStatus</code> will be
   *     returned.  If no event occurs, <code>null</code> is returned.
   * @throws SecurityException If the caller does not have permission wait for
   *     events.
   * @throws RemoteException If a communication error occurs.
   */
  JobStatus waitForJobStatusChange(long lastEventId, long timeoutMillis)
    throws SecurityException, RemoteException;
  
  /**
   * Waits for a status change for the specified job.
   * @param jobId The <code>UUID</code> identifying the job to wait on.
   * @param lastEventId The ID of the last event received, or
   *     <code>Long.MIN_VALUE</code> to indicate that no events had been
   *     received previously.
   * @param timeoutMillis The maximum amount of time (in milliseconds) to wait
   *     before returning.  If zero, then the call will return immediately.
   *     If negative, the call will wait indefinitely.
   * @return If an event has already occurred subsequent to the event with ID
   *     <code>lastEventId</code>, the pending <code>JobStatus</code> event
   *     will be returned.  Otherwise, the call will wait up to
   *     <code>timeoutMillis</code> milliseconds for an event to occur.  If
   *     one does occur in that time, that <code>JobStatus</code> will be
   *     returned.  If no event occurs, <code>null</code> is returned.
   * @throws IllegalArgumentException If no job exists with the specified
   *     <code>UUID</code>.
   * @throws SecurityException If the caller does not have permission wait for
   *     events.
   * @throws RemoteException If a communication error occurs.
   */
  JobStatus waitForJobStatusChange(UUID jobId, long lastEventId, long timeoutMillis)
      throws IllegalArgumentException, SecurityException, RemoteException;

  /**
   * Gets the current status of the specified job.
   * @param jobId The <code>UUID</code> identifying the job of which to get
   *     the status.
   * @return The current <code>JobStatus</code> for the specified job.
   * @throws IllegalArgumentException If no job exists with the specified
   *     <code>UUID</code>.
   * @throws SecurityException If the caller does not have permission to
   *     obtain the status of a job.
   * @throws RemoteException If a communication error occurs.
   */
  JobStatus getJobStatus(UUID jobId) throws IllegalArgumentException,
      SecurityException, RemoteException;

}
