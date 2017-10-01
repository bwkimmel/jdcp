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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.HostService;
import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.JobExecutionWrapper;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.remote.JobState;
import ca.eandb.jdcp.remote.JobStatus;
import ca.eandb.jdcp.remote.TaskService;
import ca.eandb.jdcp.server.classmanager.ChildClassManager;
import ca.eandb.jdcp.server.classmanager.ParentClassManager;
import ca.eandb.jdcp.server.scheduling.TaskScheduler;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.classloader.StrategyClassLoader;
import ca.eandb.util.concurrent.BackgroundThreadFactory;
import ca.eandb.util.io.FileUtil;
import ca.eandb.util.progress.CancelListener;
import ca.eandb.util.progress.ProgressMonitor;
import ca.eandb.util.progress.ProgressMonitorFactory;
import ca.eandb.util.rmi.Serialized;

/**
 * A <code>JobService</code> implementation.
 * @author Brad Kimmel
 */
public final class JobServer implements JobService {

  /**
   * The default amount of time (in seconds) to instruct workers to idle for
   * if there are no tasks to be processed.
   */
  private static final int DEFAULT_IDLE_SECONDS = 10;

  /** The <code>Logger</code> for this class. */
  private static final Logger logger = Logger.getLogger(JobServer.class);

  /** The <code>Random</code> number generator (for generating task IDs). */
  private static final Random rand = new Random();

  /**
   * The <code>ProgressMonitorFactory</code> to use to create
   * <code>ProgressMonitor</code>s for reporting overall progress of
   * individual jobs.
   * @see ca.eandb.util.progress.ProgressMonitor
   */
  private final ProgressMonitorFactory monitorFactory;

  /**
   * The <code>TaskScheduler</code> to use to select from multiple tasks to
   * assign to workers.
   */
  private final TaskScheduler scheduler;

  /**
   * The <code>ParentClassManager</code> to use for managing class
   * definitions supplied by clients.
   */
  private final ParentClassManager classManager;

  /**
   * The directory under which to provide working directories for individual
   * jobs.
   */
  private final File outputDirectory;

  /**
   * A <code>Map</code> for looking up <code>ScheduledJob</code> structures
   * by the corresponding job ID.
   * @see ca.eandb.jdcp.server.JobServer.ScheduledJob
   */
  private final Map<UUID, ScheduledJob> jobs = Collections.synchronizedMap(new HashMap<UUID, ScheduledJob>());

  /** An <code>Executor</code> to use to run asynchronous tasks. */
  private final Executor executor;

  /**
   * The <code>TaskDescription</code> to use to notify workers that no tasks
   * are available to be performed.
   */
  private TaskDescription idleTask = new TaskDescription(null, 0, DEFAULT_IDLE_SECONDS);

  private static final long POLLING_INTERVAL = 10;

  private static final TimeUnit POLLING_UNITS = TimeUnit.SECONDS;

  private final Queue<ServiceInfo> services = new LinkedList<ServiceInfo>();

  private final Map<UUID, ServiceInfo> routes = Collections.synchronizedMap(new WeakHashMap<UUID, ServiceInfo>());

  private final Map<String, ServiceInfo> hosts = Collections.synchronizedMap(new HashMap<String, ServiceInfo>());

  private final ScheduledExecutorService poller = Executors.newScheduledThreadPool(1, new BackgroundThreadFactory());

  private final DataSource dataSource = null;

  /**
   * Creates a new <code>JobServer</code>.
   * @param outputDirectory The directory to write job results to.
   * @param monitorFactory The <code>ProgressMonitorFactory</code> to use to
   *     create <code>ProgressMonitor</code>s for individual jobs.
   * @param scheduler The <code>TaskScheduler</code> to use to assign
   *     tasks.
   * @param classManager The <code>ParentClassManager</code> to use to
   *     store and retrieve class definitions.
   * @param executor The <code>Executor</code> to use to run bits of code
   *     that should not hold up the remote caller.
   */
  public JobServer(File outputDirectory, ProgressMonitorFactory monitorFactory, TaskScheduler scheduler, ParentClassManager classManager, Executor executor) throws IllegalArgumentException {
    if (!outputDirectory.isDirectory()) {
      throw new IllegalArgumentException("outputDirectory must be a directory.");
    }
    this.outputDirectory = outputDirectory;
    this.monitorFactory = monitorFactory;
    this.scheduler = scheduler;
    this.classManager = classManager;
    this.executor = executor;

    Runnable poll = new Runnable() {
      public void run() {
        pollActiveTasks();
      }
    };
    poller.scheduleAtFixedRate(poll, POLLING_INTERVAL,
        POLLING_INTERVAL, POLLING_UNITS);

    logger.info("JobServer created");
  }

  /**
   * Creates a new <code>JobServer</code>.
   * @param outputDirectory The directory to write job results to.
   * @param monitorFactory The <code>ProgressMonitorFactory</code> to use to
   *     create <code>ProgressMonitor</code>s for individual jobs.
   * @param scheduler The <code>TaskScheduler</code> to use to assign
   *     tasks.
   * @param classManager The <code>ParentClassManager</code> to use to
   *     store and retrieve class definitions.
   */
  public JobServer(File outputDirectory, ProgressMonitorFactory monitorFactory, TaskScheduler scheduler, ParentClassManager classManager) throws IllegalArgumentException {
    this(outputDirectory, monitorFactory, scheduler, classManager, Executors.newCachedThreadPool(new BackgroundThreadFactory()));
  }

  private final void pollActiveTasks() {
    for (ServiceInfo info : hosts.values()) {
      info.pollActiveTasks();
    }
  }

  @Override
  public UUID createJob(String description) throws SecurityException {
    ProgressMonitor monitor = monitorFactory.createProgressMonitor(description);
    ScheduledJob sched = new ScheduledJob(description, monitor);
    jobs.put(sched.id, sched);
    monitor.addCancelListener(new JobCancelListener(sched.id));

    if (logger.isInfoEnabled()) {
      logger.info("Job created (" + sched.id.toString() + "): " + description);
    }

    return sched.id;
  }

  @Override
  public void submitJob(Serialized<ParallelizableJob> job, UUID jobId)
      throws IllegalArgumentException, SecurityException, ClassNotFoundException, JobExecutionException {
    ScheduledJob sched = jobs.get(jobId);
    if (sched == null || sched.job != null) {
      throw new IllegalArgumentException("No pending job with provided Job ID");
    }

    try {
      ServerUtil.setHostService(sched);
      sched.initializeJob(job);
      sched.scheduleNextTask();
    } catch (JobExecutionException e) {
      handleJobExecutionException(e, jobId);
      throw e;
    } finally {
      ServerUtil.clearHostService();
    }

    if (logger.isInfoEnabled()) {
      logger.info("Pending job submitted (" + jobId.toString() + ")");
    }
  }

  @Override
  public UUID submitJob(Serialized<ParallelizableJob> job, String description)
      throws SecurityException, ClassNotFoundException, JobExecutionException {
    ProgressMonitor monitor = monitorFactory.createProgressMonitor(description);
    ScheduledJob sched = new ScheduledJob(description, monitor);
    jobs.put(sched.id, sched);
    monitor.addCancelListener(new JobCancelListener(sched.id));

    try {
      ServerUtil.setHostService(sched);
      sched.initializeJob(job);
      sched.scheduleNextTask();
    } catch (JobExecutionException e) {
      handleJobExecutionException(e, sched.id);
      throw e;
    } finally {
      ServerUtil.clearHostService();
    }

    if (logger.isInfoEnabled()) {
      logger.info("Job submitted (" + sched.id.toString() + "): "
          + description);
    }

    return sched.id;
  }

  @Override
  public void cancelJob(UUID jobId) throws IllegalArgumentException, SecurityException {
    if (!jobs.containsKey(jobId)) {
      throw new IllegalArgumentException("No job with provided Job ID");
    }

    removeScheduledJob(jobId, false);
  }

  @Override
  public Serialized<TaskWorker> getTaskWorker(UUID jobId)
      throws IllegalArgumentException, SecurityException {
    ScheduledJob sched = jobs.get(jobId);
    if (sched != null) {
      return sched.worker;
    }

    ServiceInfo info = routes.get(jobId);
    if (info != null) {
      return info.getTaskWorker(jobId);
    }

    throw new IllegalArgumentException("No submitted job with provided Job ID");
  }

  @Override
  public synchronized TaskDescription requestTask() throws SecurityException {
    TaskDescription taskDesc = scheduler.getNextTask();
    if (taskDesc != null) {
      ScheduledJob sched = jobs.get(taskDesc.getJobId());
      try {
        ServerUtil.setHostService(sched);
        sched.scheduleNextTask();
      } catch (JobExecutionException e) {
        handleJobExecutionException(e, sched.id);
      } finally {
        ServerUtil.clearHostService();
      }
      return taskDesc;
    }

    int n = services.size();
    if (n > 0) {
      ServiceInfo[] serv;
      synchronized (this) {
        serv = services.toArray(new ServiceInfo[n]);
      }
      for (ServiceInfo info : serv) {
        try {
          synchronized (this) {
            if (services.remove(info)) {
              services.add(info);
            }
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

  @Override
  public void submitTaskResults(final UUID jobId, final int taskId,
      final Serialized<Object> results) throws SecurityException {
    ScheduledJob sched = jobs.get(jobId);
    if (sched != null) {
      try {
        ServerUtil.setHostService(sched);
        sched.submitTaskResults(taskId, results);
      } finally {
        ServerUtil.clearHostService();
      }
      return;
    }

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

  @Override
  public void reportException(final UUID jobId, final int taskId, final Exception e)
      throws SecurityException, RemoteException {
    ScheduledJob sched = jobs.get(jobId);
    if (sched != null) {
      sched.reportException(taskId, e);
    }

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

  @Override
  public BitSet getFinishedTasks(UUID[] jobIds, int[] taskIds)
      throws IllegalArgumentException, SecurityException, RemoteException {

    if (jobIds == null || taskIds == null) {
      return new BitSet(0);
    }

    if (jobIds.length != taskIds.length) {
      throw new IllegalArgumentException("jobIds.length != taskIds.length");
    }

    BitSet finished = new BitSet(jobIds.length);

    for (int i = 0; i < jobIds.length; i++) {
      UUID jobId = jobIds[i];
      int taskId = taskIds[i];
      if (taskId != 0) {
        if (jobs.containsKey(jobId)) {
          finished.set(i, jobId == null || !scheduler.contains(jobId, taskId));
        } else {
          ServiceInfo info = routes.get(jobId);
          finished.set(i, (info == null) || info.isTaskComplete(jobId, taskId));
        }
      } else {
        ScheduledJob sched = jobs.get(jobId);
        if (sched != null) {
          try {
            finished.set(i, sched.job.isComplete());
          } catch (JobExecutionException e) {
            sched.reportException(0, e);
          }
        } else {
          finished.set(i, !routes.containsKey(jobId));
        }
      }
    }

    return finished;

  }

  @Override
  public byte[] getClassDefinition(String name, UUID jobId)
      throws SecurityException {
    ScheduledJob sched = jobs.get(jobId);
    if (sched != null) {
      ByteBuffer def = sched.classManager.getClassDefinition(name);
      if (def.hasArray() && def.arrayOffset() == 0) {
        return def.array();
      } else {
        byte[] bytes = new byte[def.remaining()];
        def.get(bytes);
        return bytes;
      }
    }

    ServiceInfo info = routes.get(jobId);
    if (info != null) {
      return info.getClassDefinition(name, jobId);
    }

    throw new IllegalArgumentException("No job with provided Job ID");
  }

  @Override
  public byte[] getClassDigest(String name, UUID jobId)
      throws SecurityException {
    ScheduledJob sched = jobs.get(jobId);
    if (sched != null) {
      return sched.classManager.getClassDigest(name);
    }

    ServiceInfo info = routes.get(jobId);
    if (info != null) {
      return info.getClassDigest(name, jobId);
    }

    throw new IllegalArgumentException("No job with provided Job ID");
  }

  @Override
  public byte[] getClassDigest(String name) throws SecurityException {
    return classManager.getClassDigest(name);
  }

  @Override
  public void setClassDefinition(String name, byte[] def)
      throws SecurityException {
    classManager.setClassDefinition(name, def);

    if (logger.isInfoEnabled()) {
      logger.info("Global class definition updated for " + name);
    }
  }

  @Override
  public void setClassDefinition(String name, UUID jobId, byte[] def)
      throws IllegalArgumentException, SecurityException {
    ScheduledJob sched = jobs.get(jobId);
    if (sched == null || sched.job != null) {
      throw new IllegalArgumentException("No pending job with provided Job ID");
    }

    sched.classManager.setClassDefinition(name, def);

    if (logger.isInfoEnabled()) {
      logger.info("Class definition of " + name + " set for job "
          + jobId.toString());
    }
  }

  @Override
  public void setIdleTime(int idleSeconds) throws IllegalArgumentException,
      SecurityException {
    idleTask = new TaskDescription(null, 0, idleSeconds);
    if (logger.isInfoEnabled()) {
      logger.info("Idle time set to " + Integer.toString(idleSeconds));
    }
  }

  @Override
  public void setJobPriority(UUID jobId, int priority)
      throws IllegalArgumentException, SecurityException {
    if (!jobs.containsKey(jobId)) {
      throw new IllegalArgumentException("No job with provided Job ID");
    }

    scheduler.setJobPriority(jobId, priority);
    if (logger.isInfoEnabled()) {
      logger.info("Set job " + jobId.toString() + " priority to "
          + Integer.toString(priority));
    }
  }

  /**
   * Handles a <code>JobExcecutionException</code> thrown by a job managed
   * by this server.
   * @param e The <code>JobExecutionException</code> that was thrown by the
   *     job.
   * @param jobId The <code>UUID</code> identifying the job that threw the
   *     exception.
   */
  private void handleJobExecutionException(JobExecutionException e, UUID jobId) {
    logger.error("Exception thrown from job " + jobId.toString(), e);
    removeScheduledJob(jobId, false);
  }

  /**
   * Removes a job.
   * @param jobId The <code>UUID</code> identifying the job to be removed.
   * @param complete A value indicating whether the job has been completed.
   */
  private void removeScheduledJob(UUID jobId, boolean complete) {
    ScheduledJob sched = jobs.remove(jobId);
    if (sched != null) {
      if (complete) {
        sched.notifyComplete();
        if (logger.isInfoEnabled()) {
          logger.info("Job complete (" + jobId.toString() + ")");
        }
      } else {
        sched.notifyCancelled();
        if (logger.isInfoEnabled()) {
          logger.info("Job cancelled (" + jobId.toString() + ")");
        }
      }
      jobs.remove(jobId);
      scheduler.removeJob(jobId);
      sched.classManager.release();
    }
  }

  @Override
  public void registerTaskService(String name, TaskService service)
      throws SecurityException, RemoteException {
    if (hosts.containsKey(name)) {
      unregisterTaskService(name);
    }
    ServiceInfo info = new ServiceInfo(service, dataSource, executor);
    hosts.put(name, info);
    services.add(info);
  }

  @Override
  public void unregisterTaskService(String name) throws SecurityException,
      RemoteException {
    ServiceInfo info = hosts.get(name);
    if (info != null) {
      hosts.remove(name);
      services.remove(info);
      synchronized (routes) {
        for (Entry<UUID, ServiceInfo> entry : routes.entrySet()) {
          if (entry.getValue() == info) {
            routes.remove(entry.getKey());
          }
        }
      }
      info.shutdown();
    }
  }

  /**
   * Represents a <code>ParallelizableJob</code> that has been submitted
   * to this <code>JobMasterServer</code>.
   * @author Brad Kimmel
   */
  private class ScheduledJob implements HostService, ProgressMonitor {

    /** The <code>ParallelizableJob</code> to be processed. */
    public JobExecutionWrapper job;

    /** The <code>UUID</code> identifying the job. */
    public final UUID id;

    /** A description of the job. */
    public final String description;

    /** The <code>TaskWorker</code> to use to process tasks for the job. */
    public Serialized<TaskWorker> worker;

    /**
     * The <code>ProgressMonitor</code> to use to monitor the progress of
     * the <code>Job</code>.
     */
    public final ProgressMonitor monitor;

    /**
     * The <code>ClassManager</code> to use to store the class definitions
     * applicable to this job.
     */
    public final ChildClassManager classManager;

    /** The working directory for this job. */
    private final File workingDirectory;

    /** The <code>ClassLoader</code> to use to deserialize this job. */
    public ClassLoader classLoader;

    /** A value indicating if the last attempt to obtain a task failed. */
    private boolean stalled = false;

    /**
     * Initializes the scheduled job.
     * @param description A description of the job.
     * @param monitor The <code>ProgressMonitor</code> to use to monitor
     *     the progress of the <code>ParallelizableJob</code>.
     */
    public ScheduledJob(String description, ProgressMonitor monitor) {

      this.id = UUID.randomUUID();
      this.description = description;

      //String title = String.format("%s (%s)", this.job.getClass().getSimpleName(), this.id.toString());
      this.monitor = monitor;
      this.monitor.notifyStatusChanged("Awaiting job submission");

      this.classManager = JobServer.this.classManager.createChildClassManager();

      this.workingDirectory = new File(outputDirectory, id.toString());

      setJobStatus(new JobStatus(id, description, JobState.NEW, 0.0,
          "Awaiting job submission"));

    }

    /**
     * Gets the current status of this job.
     * @return The current <code>JobStatus</code> for this job.
     */
    private JobStatus getJobStatus() {
      return statusByJobId.get(id);
    }

    /**
     * Sets the status of this job.
     * @param newStatus The new <code>JobStatus</code> for this job.
     */
    private synchronized void setJobStatus(JobStatus newStatus) {
      updateStatus(newStatus);
      notifyAll();  // wake up any listeners.
    }

    /**
     * Wait for a status change for this job.
     * @param lastEventId The ID of the last event received, or
     *     <code>Long.MIN_VALUE</code> to indicate that no events had been
     *     received previously.
     * @param timeoutMillis The maximum amount of time (in milliseconds) to
     *     wait before returning.  If zero, then the call will return
     *     immediately.  If negative, the call will wait indefinitely.
     * @return If an event has already occurred subsequent to the event with
     *     ID <code>lastEventId</code>, the pending <code>JobStatus</code>
     *     event will be returned.  Otherwise, the call will wait up to
     *     <code>timeoutMillis</code> milliseconds for an event to occur.
     *     If one does occur in that time, that <code>JobStatus</code> will
     *     be returned.  If no event occurs, <code>null</code> is returned.
     */
    public synchronized JobStatus waitForJobStatusChange(long lastEventId, long timeoutMillis) {

      // when should I time out?
      long end = timeoutMillis >= 0 ? System.currentTimeMillis() + timeoutMillis : Long.MAX_VALUE;

      // wait loop
      while (getJobStatus().getEventId() <= lastEventId) {
        try {
          long time = System.currentTimeMillis();
          if (time >= end) {
            return null;    // timeout
          }
          wait(end - time);
        } catch (InterruptedException e) { /* nothing to do. */ }
      }

      // we found a newer event.
      return getJobStatus();

    }

    /**
     * Deserializes the job and prepares it to be managed by the host
     * machine.
     * @param job The serialized job.
     * @throws ClassNotFoundException If a class required by the job is
     *     missing.
     * @throws JobExecutionException If the job throws an exception.
     */
    public void initializeJob(Serialized<ParallelizableJob> job) throws ClassNotFoundException, JobExecutionException {
      this.classLoader = new StrategyClassLoader(classManager, JobServer.class.getClassLoader());
      this.job = new JobExecutionWrapper(job.deserialize(classLoader));
      this.worker = new Serialized<TaskWorker>(this.job.worker());
      notifyStatusChanged("");

      this.workingDirectory.mkdir();
      this.job.setHostService(this);

      File logFile = new File(workingDirectory, "job.log");
      PrintStream log;
      try {
        Date now = new Date();
        log = new PrintStream(new FileOutputStream(logFile, true));
        log.printf("%tc: Job %s submitted.", now, id.toString());
        log.println();
        log.printf("%tc: Description: ", now);
        log.println(description);
        log.flush();
        log.close();
      } catch (FileNotFoundException e) {
        logger.error("Unable to open job log file.", e);
      }

      this.job.initialize();
    }

    /**
     * Submits the results for a task associated with this job.
     * @param taskId The ID of the task whose results are being submitted.
     * @param results The serialized results.
     */
    public void submitTaskResults(int taskId, Serialized<Object> results) {
      TaskDescription taskDesc = scheduler.remove(id, taskId);
      if (taskDesc != null) {
        Object task = taskDesc.getTask().get();
        Runnable command = new TaskResultSubmitter(this, task, results, this);
        try {
          executor.execute(command);
        } catch (RejectedExecutionException e) {
          command.run();
        }
      }
    }

    /**
     * Reports an exception thrown by a worker while processing a task for
     * this job.
     * @param taskId The ID of the task that was being processed.
     * @param ex The exception that was thrown.
     */
    public synchronized void reportException(int taskId, Exception ex) {
      PrintStream log = null;

      try {
        File logFile = new File(workingDirectory, "job.log");
        log = new PrintStream(new FileOutputStream(logFile, true));
        if (taskId != 0) {
          log.printf("%tc: A worker reported an exception while processing the job:", new Date());
        } else {
          log.printf("%tc: A worker reported an exception while processing a task (%d):", new Date(), taskId);
        }
        log.println();
        ex.printStackTrace(log);
      } catch (IOException e) {
        logger.error("Exception thrown while logging exception for job " + id.toString(), e);
      } finally {
        if (log != null) {
          log.close();
        }
      }

    }

    /**
     * Generates a unique task identifier.
     * @return The generated task ID.
     */
    private int generateTaskId() {
      int taskId;
      do {
        taskId = rand.nextInt();
      } while (taskId != 0 && scheduler.contains(id, taskId));
      return taskId;
    }

    /**
     * Obtains and schedules the next task for this job.
     * @throws JobExecutionException If the job throws an exception while
     *     attempting to obtain the next task.
     */
    public void scheduleNextTask() throws JobExecutionException {
      Object task = job.getNextTask();
      stalled = (task == null);
      if (!stalled) {
        int taskId = generateTaskId();
        TaskDescription desc = new TaskDescription(id, taskId, task);
        scheduler.add(desc);
      }
    }

    /**
     * Writes the results of a <code>ScheduledJob</code> to the output
     * directory.
     * @throws JobExecutionException If the job throws an exception.
     */
    private synchronized void finalizeJob() throws JobExecutionException {

      assert(job.isComplete());

      job.finish();

      try {

        String filename = String.format("%s.zip", id.toString());
        File outputFile = new File(outputDirectory, filename);

        File logFile = new File(workingDirectory, "job.log");
        PrintStream log = new PrintStream(new FileOutputStream(logFile, true));

        log.printf("%tc: Job %s completed.", new Date(), id.toString());
        log.println();
        log.flush();
        log.close();

        FileUtil.zip(outputFile, workingDirectory);
        FileUtil.deleteRecursive(workingDirectory);

      } catch (IOException e) {
        logger.error("Exception caught while finalizing job " + id.toString(), e);
      }

    }

    /**
     * Gets the <code>File</code> associated with the specified path which
     * is relative to this jobs working directory.
     * @param path The relative path.
     * @return The <code>File</code> associated with the specified path.
     * @throws IllegalArgumentException If the path is not a relative path
     *     or references the parent directory (..).
     */
    private File getWorkingFile(String path) {
      File file = new File(workingDirectory, path).getAbsoluteFile();
      try {
        if (!FileUtil.isAncestor(file, workingDirectory)) {
          throw new IllegalArgumentException("path must not reference parent directory.");
        }
      } catch (IOException e) {
        throw new UnexpectedException(e);
      }
      return file;
    }

    @Override
    public FileOutputStream createFileOutputStream(final String path) {
      return AccessController.doPrivileged(new PrivilegedAction<FileOutputStream>() {
        public FileOutputStream run() {
          File file = getWorkingFile(path);
          File dir = file.getParentFile();
          dir.mkdirs();
          try {
            return new FileOutputStream(file);
          } catch (FileNotFoundException e) {
            throw new UnexpectedException(e);
          }
        }
      });
    }

    @Override
    public RandomAccessFile createRandomAccessFile(final String path) {
      return AccessController.doPrivileged(new PrivilegedAction<RandomAccessFile>() {
        public RandomAccessFile run() {
          File file = getWorkingFile(path);
          File dir = file.getParentFile();
          dir.mkdirs();
          try {
            return new RandomAccessFile(file, "rw");
          } catch (FileNotFoundException e) {
            throw new UnexpectedException(e);
          }
        }
      });
    }

    @Override
    public boolean notifyProgress(int value, int maximum) {
      setJobStatus(getJobStatus().withProgress((double) value / (double) maximum));
      return monitor.notifyProgress(value, maximum);
    }

    @Override
    public boolean notifyProgress(double progress) {
      setJobStatus(getJobStatus().withProgress(progress));
      return monitor.notifyProgress(progress);
    }

    @Override
    public boolean notifyIndeterminantProgress() {
      setJobStatus(getJobStatus().withIndeterminantProgress());
      return monitor.notifyIndeterminantProgress();
    }

    @Override
    public void notifyComplete() {
      setJobStatus(getJobStatus().asComplete());
      monitor.notifyComplete();
    }

    @Override
    public void notifyCancelled() {
      setJobStatus(getJobStatus().asCancelled());
      monitor.notifyCancelled();
    }

    @Override
    public void notifyStatusChanged(String newStatus) {
      setJobStatus(getJobStatus().withStatus(newStatus));
      monitor.notifyStatusChanged(newStatus);
    }

    @Override
    public boolean isCancelPending() {
      return monitor.isCancelPending();
    }

    @Override
    public void addCancelListener(CancelListener listener) {
      monitor.addCancelListener(listener);
    }

  }

  /**
   * A <code>Runnable</code> task for submitting task results asynchronously.
   * @author Brad Kimmel
   */
  private class TaskResultSubmitter implements Runnable {

    /**
     * The <code>ScheduledJob</code> associated with the task whose results
     * are being submitted.
     */
    private final ScheduledJob sched;

    /**
     * The <code>Object</code> describing the task whose results are being
     * submitted.
     */
    private final Object task;

    /** The serialized task results. */
    private final Serialized<Object> results;

    /** The <code>ProgressMonitor</code> to report job progress to. */
    private final ProgressMonitor monitor;

    /**
     * Creates a new <code>TaskResultSubmitter</code>.
     * @param sched The <code>ScheduledJob</code> associated with the task
     *     whose results are being submitted.
     * @param task The <code>Object</code> describing the task whose
     *     results are being submitted.
     * @param results The serialized task results.
     * @param monitor The <code>ProgressMonitor</code> to report job
     *     progress to.
     */
    public TaskResultSubmitter(ScheduledJob sched, Object task,
        Serialized<Object> results, ProgressMonitor monitor) {
      this.sched = sched;
      this.task = task;
      this.results = results;
      this.monitor = monitor;
    }

    @Override
    public void run() {
      ClassLoader cl = sched.classLoader;
      if (task != null) {
        try {
          ServerUtil.setHostService(sched);
          sched.job.submitTaskResults(task,
              results.deserialize(cl), monitor);

          if (sched.job.isComplete()) {
            sched.finalizeJob();
            removeScheduledJob(sched.id, true);
          } else {
            synchronized (sched) {
              if (sched.stalled) {
                sched.scheduleNextTask();
              }
            }
          }
        } catch (JobExecutionException e) {
          handleJobExecutionException(e, sched.id);
        } catch (ClassNotFoundException e) {
          logger.error(
              "Exception thrown submitting results of task for job "
                  + sched.id.toString(), e);
          removeScheduledJob(sched.id, false);
        } catch (Exception e) {
          logger.error(
              "Exception thrown while attempting to submit task results for job "
                  + sched.id.toString(), e);
          removeScheduledJob(sched.id, false);
        } finally {
          ServerUtil.clearHostService();
        }
      }
    }

  }

  /**
   * Cancels a job when notified.
   * @author Brad Kimmel
   */
  private class JobCancelListener implements CancelListener {

    /** The <code>UUID</code> identifying the job to be cancelled. */
    private final UUID jobId;

    /**
     * Creates a new <code>JobCancelListener</code>.
     * @param jobId The <code>UUID</code> identifying the job to be
     *   cancelled.
     */
    public JobCancelListener(UUID jobId) {
      this.jobId = jobId;
    }

    @Override
    public void cancelRequested() {
      cancelJob(jobId);
    }

  }

  /** Events indexed and sorted by event ID. */
  private SortedMap<Long, JobStatus> statusByEventId =
      Collections.synchronizedSortedMap(new TreeMap<Long, JobStatus>());

  /** Events indexed by job. */
  private Map<UUID, JobStatus> statusByJobId =
      Collections.synchronizedMap(new HashMap<UUID, JobStatus>());

  /**
   * Submits a new <code>JobStatus</code> event.
   * @param _newStatus The new <code>JobStatus</code> event.
   */
  private synchronized void updateStatus(JobStatus _newStatus) {

    // generate a new event ID (always increasing).
    JobStatus newStatus = _newStatus.withNewEventId();

    UUID jobId = newStatus.getJobId();

    // remove the last event for the same job, as it is no longer relevant.
    JobStatus oldStatus = statusByJobId.get(jobId);
    if (oldStatus != null) {
      statusByEventId.remove(oldStatus.getEventId());
    }

    // insert event into lookup tables.
    statusByEventId.put(newStatus.getEventId(), newStatus);
    statusByJobId.put(jobId, newStatus);

    // wake up any listeners.
    notifyAll();

  }

  @Override
  public synchronized JobStatus waitForJobStatusChange(long lastEventId, long timeoutMillis)
      throws SecurityException, RemoteException {

    // when should I time out?
    long end = timeoutMillis >= 0 ? System.currentTimeMillis() + timeoutMillis : Long.MAX_VALUE;

    // wait loop
    SortedMap<Long, JobStatus> tail;
    while (true) {
      // find events newer than the last event the caller has seen.
      tail = statusByEventId.tailMap(lastEventId + 1);
      if (!tail.isEmpty()) break;
      try {
        long time = System.currentTimeMillis();
        if (time >= end) {
          return null;    // timeout
        }
        wait(end - time);
      } catch (InterruptedException e) { /* nothing to do. */ }
    }

    // found an event.
    return tail.values().iterator().next();

  }

  @Override
  public JobStatus waitForJobStatusChange(UUID jobId, long lastEventId, long timeoutMillis)
      throws IllegalArgumentException, SecurityException, RemoteException {


    // try to find the job if it is still active.
    ScheduledJob sched = jobs.get(jobId);

    // if the job is no longer active, it could be that the caller has not
    // yet received the COMPLETE/CANCELLED status, which will still be in
    // the lookup.
    if (sched == null) {
      JobStatus status = statusByJobId.get(jobId);
      if (status == null || lastEventId >= status.getEventId()) {
        // the job either never existed, or the caller has already seen
        // the COMPLETE event for this job.
        throw new IllegalArgumentException("Invalid Job ID");
      }
      return status;
    }

    // delegate to active job to wait for the event.
    return sched.waitForJobStatusChange(lastEventId, timeoutMillis);

  }

  @Override
  public JobStatus getJobStatus(UUID jobId) throws IllegalArgumentException,
      SecurityException, RemoteException {
    if (!statusByJobId.containsKey(jobId)) {
      throw new IllegalArgumentException("Invalid job ID");
    }
    return statusByJobId.get(jobId);
  }

}
