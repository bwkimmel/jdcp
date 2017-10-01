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

import java.sql.SQLException;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.DelegationException;
import ca.eandb.jdcp.worker.policy.CourtesyMonitor;
import ca.eandb.jdcp.worker.policy.UnconditionalCourtesyMonitor;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.classloader.ClassLoaderStrategy;
import ca.eandb.util.classloader.StrategyClassLoader;
import ca.eandb.util.progress.CancelListener;
import ca.eandb.util.progress.CompositeCancelListener;
import ca.eandb.util.progress.ProgressMonitor;
import ca.eandb.util.progress.ProgressMonitorFactory;
import ca.eandb.util.rmi.Serialized;

/**
 * A <code>Runnable</code> worker that processes tasks for a
 * <code>ParallelizableJob</code> from a remote <code>JobServiceMaster</code>.
 * This class may potentially use multiple threads to process tasks.
 * @author Brad Kimmel
 */
public final class ThreadServiceWorker implements Runnable {

  public ThreadServiceWorker(JobServiceFactory serviceFactory, ThreadFactory threadFactory, ProgressMonitorFactory monitorFactory, CourtesyMonitor courtesyMonitor) {

    assert(maxWorkers > 0);

    this.service = new ReconnectingJobService(serviceFactory);
    this.executor = Executors.newCachedThreadPool(threadFactory);
    this.maxWorkers = Runtime.getRuntime().availableProcessors();
    this.monitorFactory = monitorFactory;
    this.courtesyMonitor = courtesyMonitor;

  }

  public ThreadServiceWorker(JobServiceFactory serviceFactory, ThreadFactory threadFactory, ProgressMonitorFactory monitorFactory) {
    this(serviceFactory, threadFactory, monitorFactory, new UnconditionalCourtesyMonitor());
  }

  /**
   * Sets a <code>DataSource</code> to use to store cached class definitions.
   * @param dataSource A <code>DataSource</code> to use to store cached class
   *     definitions.
   * @throws SQLException If an error occurs while initializing the data
   *     source.
   */
  public void setDataSource(DataSource dataSource) throws SQLException {
    DbCachingJobServiceClassLoaderStrategy.prepareDataSource(dataSource);
    this.dataSource = dataSource;
  }

  @Override
  public synchronized void run() {

    runThread = Thread.currentThread();

    FinishedTaskPoller poller = new FinishedTaskPoller();
    executor.execute(poller);

    while (!shutdownPending) {
      try {
        Worker worker = getWorker();
        executor.execute(worker);
      } catch (InterruptedException e) {
        /* nothing to do. */
      }
    }

    poller.shutdown();
    runThread = null;

  }

  private class FinishedTaskPoller implements Runnable {

    private boolean shutdown = false;

    private Thread pollingThread = null;

    public synchronized void shutdown() {
      shutdown = true;
      Thread thread = pollingThread;
      if (thread != null) {
        thread.interrupt();
      }
    }

    public void run() {
      pollingThread = Thread.currentThread();

      Worker[] workers;
      UUID[] jobIds;
      int[] taskIds;
      boolean lastPollOk = true;
      int nThreads, nJobs;
      boolean removedJob;

      while (!shutdown) {
        synchronized (activeWorkers) {
          synchronized (workerMap) {
            nThreads = activeWorkers.size();
            nJobs = workerMap.size();
            workers = new Worker[nThreads];
            jobIds = new UUID[nThreads + nJobs];
            taskIds = new int[nThreads + nJobs];
            int i = 0;
            for (Worker worker : activeWorkers) {
              workers[i] = worker;
              jobIds[i] = worker.getCurrentJobId();
              taskIds[i++] = worker.getCurrentTaskId();
            }
            for (UUID jobId : workerMap.keySet()) {
              jobIds[i] = jobId;
              taskIds[i++] = 0;
            }
          }
        }

        removedJob = false;
        if (taskIds.length > 0) {
          try {
            BitSet finished = service.getFinishedTasks(jobIds, taskIds);
            lastPollOk = true;
            for (int i = finished.nextSetBit(0); i >= 0; i = finished
                .nextSetBit(i + 1)) {
              if (i < nThreads) {
                workers[i].cancel(jobIds[i], taskIds[i]);
              } else {
                workerMap.remove(jobIds[i]);
                removedJob = true;
              }
            }
          } catch (Exception e) {
            if (lastPollOk) {
              logger.warn("Could not poll for finished tasks.", e);
              lastPollOk = false;
            }
          }
        }

        if (removedJob) {
          System.gc();
        }

        try {
          Thread.sleep(finishedTaskPollingInterval);
        } catch (InterruptedException e) {}
      }
      pollingThread = null;
    }
  }

  /**
   * Shuts down the <code>Thread</code> currently processing this worker.
   */
  public void shutdown() {
    synchronized (runThread) {
      if (runThread != null && !shutdownPending) {
        shutdownPending = true;
        runThread.interrupt();
      }
    }
  }

  /**
   * Sets the maximum number of concurrent workers.
   * @param maxWorkers The maximum number of concurrent workers.
   */
  public void setMaxWorkers(int maxWorkers) {
    synchronized (workerQueue) {
      idleLock.lock();
      try {
        int oldMaxWorkers = this.maxWorkers;
        this.maxWorkers = maxWorkers;

        // If the number of workers is being reduced, then signal any
        // waiting workers so that they can terminate themselves if
        // their id is greater than the number of workers, and so that
        // another worker can take over idle polling if the current
        // polling worker is to be terminated.
        if (maxWorkers < oldMaxWorkers) {
          idleComplete.signalAll();
        }
      } finally {
        idleLock.unlock();
      }
      while (numWorkers < maxWorkers) {
        String title = String.format("Worker (%d)", numWorkers + 1);
        ProgressMonitorWrapper monitor = new ProgressMonitorWrapper(numWorkers++, monitorFactory.createProgressMonitor(title));
        workerQueue.add(new Worker(monitor));
      }
    }
  }

  /**
   * Gets the next worker available to process a task.
   * @return The next available worker.
   * @throws InterruptedException If the thread is interrupted while waiting
   *     for an available worker.
   */
  private Worker getWorker() throws InterruptedException {
    while (!courtesyMonitor.allowTasksToRun()) {
      courtesyMonitor.waitFor();
    }
    while (numWorkers > maxWorkers) {
      workerQueue.take();
      numWorkers--;
    }
    return workerQueue.take();
  }

  /**
   * Reference to a <code>TaskWorker</code>.  This object acts as a handle
   * for other workers to synchronize on to prevent multiple worker threads
   * from trying to download the same <code>TaskWorker</code>.
   *
   * @author Brad Kimmel
   */
  private static class TaskWorkerInfo {
    public TaskWorker worker;
    public ClassLoader loader;
  };

  /** A <code>Map</code> containing the active <code>TaskWorker</code>s. */
  private final Map<UUID, TaskWorkerInfo> workerMap =
      Collections.synchronizedMap(new HashMap<UUID, TaskWorkerInfo>());

  /**
   * Obtains the task worker to process tasks for the job with the specified
   * <code>UUID</code>.
   * @param jobId The <code>UUID</code> of the job to obtain the task worker
   *     for.
   * @return The <code>TaskWorkerInfo</code> to process tasks for the job with
   *     the specified <code>UUID</code>, or <code>null</code> if the job
   *     is invalid or has already been completed.
   * @throws ClassNotFoundException
   */
  private TaskWorkerInfo getTaskWorker(UUID jobId) throws ClassNotFoundException {

    /* First try to get the worker from the local map. */
    TaskWorkerInfo info;
    synchronized (workerMap) {
      info = workerMap.get(jobId);
      if (info == null) {
        info = new TaskWorkerInfo();
        workerMap.put(jobId, info);
      }
    }

    synchronized (info) {
      if (info.worker == null) {

        /* The task worker was not in the cache, so use the service to
         * obtain the task worker.
         */
        Serialized<TaskWorker> envelope = this.service.getTaskWorker(jobId);

        ClassLoaderStrategy strategy;
        if (dataSource != null) {
          strategy = new DbCachingJobServiceClassLoaderStrategy(
              service, jobId, dataSource);
        } else {
          strategy = new InternalCachingJobServiceClassLoaderStrategy(
              service, jobId);
        }

        info.loader = new StrategyClassLoader(
            strategy, ThreadServiceWorker.class.getClassLoader());
        info.worker = envelope.deserialize(info.loader);

        if (logger.isInfoEnabled()) {
          logger.info(String.format(
              "Got worker (thread=%d)", Thread.currentThread().getId()));
        }

      }
    }

    assert(info.worker != null);
    return info;

  }

  /**
   * Used to process tasks in threads.
   * @author Brad Kimmel
   */
  private class Worker implements Runnable {

    /**
     * Initializes the progress monitor to report to.
     * @param monitor The <code>ProgressMonitor</code> to report
     *     the progress of the task to.
     */
    public Worker(ProgressMonitorWrapper monitor) {
      this.monitor = monitor;
    }

    /**
     * Signals this worker that it should cancel the currently running
     * task if it the specified task.
     * @param jobId The <code>UUID</code> of the job whose task is to be
     *     cancelled.
     * @param taskId The ID of the task to be cancelled.
     */
    public void cancel(UUID jobId, int taskId) {
      if (jobId == currentJobId && taskId == currentTaskId) {
        monitor.cancel();
      }
    }

    public UUID getCurrentJobId() {
      return currentJobId;
    }

    public int getCurrentTaskId() {
      return currentTaskId;
    }

    @Override
    public void run() {

      try {

        this.monitor.reset();
        this.monitor.notifyIndeterminantProgress();
        this.monitor.notifyStatusChanged("Requesting task...");

        if (service != null) {

          // Wait for idling to complete.
          if (!idleWait()) {
            return; // Monitor signaled worker should cancel.
          }

          TaskDescription taskDesc = service.requestTask();
          UUID jobId = taskDesc.getJobId();
          int taskId = taskDesc.getTaskId();

          if (jobId != null) { // server has a task to perform.

            idleEnd(); // Signal that idling is complete.
            currentJobId = jobId;
            currentTaskId = taskId;
            activeWorkers.add(this);

            this.monitor.notifyStatusChanged("Obtaining task worker...");
            TaskWorkerInfo info;
            try {
              info = getTaskWorker(jobId);
            } catch (DelegationException e) {
              info = null;
            } catch (ClassNotFoundException e) {
              service.reportException(jobId, 0, e);
              idle(EXCEPTION_IDLE_SECONDS, EXCEPTION_IDLE_MESSAGE);
              info = null;
            }

            if (info == null) {
              this.monitor.notifyStatusChanged("Could not obtain worker...");
              this.monitor.notifyCancelled();
              return;
            }

            this.monitor.notifyStatusChanged("Performing task...");
            Object results;

            try {
              Object task = taskDesc.getTask().deserialize(info.loader);
              results = info.worker.performTask(task, monitor);
            } catch (DelegationException e) {
              results = null;
            } catch (Exception e) {
              service.reportException(jobId, taskId, e);
              idle(EXCEPTION_IDLE_SECONDS, EXCEPTION_IDLE_MESSAGE);
              results = null;
            }

            if (results != null && !monitor.isCancelPending()) {
              this.monitor.notifyStatusChanged("Submitting task results...");
              service.submitTaskResults(jobId, taskId, new Serialized<Object>(results));
            }

          } else { // server has no tasks to perform.

            if (idleBegin()) {
              try {
                int seconds = (Integer) taskDesc.getTask().deserialize();
                this.idle(seconds);
              } catch (ClassNotFoundException e) {
                throw new UnexpectedException(e);
              }
            }

          }

        }

      } finally {

        this.monitor.notifyComplete();
        activeWorkers.remove(this);
        currentJobId = null;
        currentTaskId = 0;
        workerQueue.add(this);

      }

    }

    /**
     * Enter idling state.
     * @return A value indicating whether the current thread is designated
     *     to poll the server.
     */
    private boolean idleBegin() {
      /* Only a single worker should idle.  The first one to
       * get here will be designated to poll the server.
       */
      idleLock.lock();
      if (!idling) {
        idling = true;
        poller = monitor.workerId;
      }
      idleLock.unlock();

      return (poller == monitor.workerId); // did we win the race?
    }

    /**
     * Exit idling state, if it is enabled.
     */
    private void idleEnd() {
      /* If this task was designated to poll the server while
       * idling, then we should disable the idling flag and
       * wake up the other workers so that they can start
       * processing tasks.
       */
      if (poller == monitor.workerId) {
        idleLock.lock();
        try {
          idling = false;
          poller = -1;
          idleComplete.signalAll();
        } finally {
          idleLock.unlock();
        }
      }
    }

    /**
     * Wait for idling to complete if the current thread is not designated
     * to poll the server.  This method blocks until the current thread
     * should continue.
     * @return If true, the calling worker may proceed.  If false, the
     *     worker should terminate.
     */
    private boolean idleWait() {
      idleLock.lock();
      try {

        /* If we are currently idling and this worker is not
         * designated to poll the server, then wait until the
         * polling worker signals me that the server has tasks
         * to process.
         */
        if (idling && poller != monitor.workerId) {
          monitor.notifyStatusChanged("Waiting...");
          do {

            /* Update the progress monitor and check if this
             * worker should terminate.
             */
            if (!monitor.notifyIndeterminantProgress()) {
              return false;
            }

            try {
              // Wait on the condition.
              idleComplete.await();

              /* At this point, either:
               * 1) The number of workers was reduced (the
               *    "idling" flag will be true).
               *      Check if the polling worker is being
               *      terminated.  If so, and if this
               *      worker is not being terminated, then
               *      take over polling duties.
               * 2) The polling worker is signalling that
               *    the server has tasks to perform (the
               *    "idling" flag will be false).
               *       Exit the wait look and request a
               *       task from the server.
               */
              if (idling && poller >= maxWorkers
                  && monitor.workerId < maxWorkers) {
                poller = monitor.workerId;
                break;
              }
            } catch (InterruptedException e) {}
          } while (idling);
        }
      } finally {
        idleLock.unlock();
      }

      return true;
    }

    /**
     * Idles for the specified number of seconds.
     * @param seconds The number of seconds to idle for.
     */
    private void idle(int seconds) {
      idle(seconds, DEFAULT_IDLE_MESSAGE);
    }

    /**
     * Idles for the specified number of seconds.
     * @param seconds The number of seconds to idle for.
     * @param message The message to display on the
     *   <code>ProgressMonitor</code> while idling.
     */
    private void idle(int seconds, String message) {

      monitor.notifyStatusChanged(message);

      for (int i = 0; i < seconds; i++) {

        if (!monitor.notifyProgress(i, seconds)) {
          monitor.notifyCancelled();
          return;
        }

        this.sleep();

      }

      monitor.notifyProgress(seconds, seconds);
      monitor.notifyComplete();

    }

    /**
     * Sleeps for one second.
     */
    private void sleep() {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("Thread was interrupted", e);
      }
    }

    /**
     * The <code>ProgressMonitor</code> to report to.
     */
    private final ProgressMonitorWrapper monitor;

    private UUID currentJobId = null;

    private int currentTaskId = 0;

  }

  /**
   * A <code>ProgressMonitor</code> that wraps another to signal cancellation
   * when the <code>ThreadServiceWorker</code> is shutting down.
   * @author Brad Kimmel
   */
  private class ProgressMonitorWrapper implements ProgressMonitor {

    /** The <code>ProgressMonitor</code> to wrap. */
    private final ProgressMonitor monitor;

    /**
     * The id for this worker (used to determine if the worker should shut
     * down if the maximum number of concurrent workers is reduced).
     */
    private final int workerId;

    /** A value indicating if the task is pending cancellation. */
    private boolean cancelPending = false;

    /**
     * The <code>CancelListener</code> to be notified if the operation is
     * to be cancelled.
     */
    private CompositeCancelListener cancelListeners = new CompositeCancelListener();

    /**
     * Creates a new <code>ProgressMonitorWrapper</code>.
     * @param workerId The id of this worker (used to determine if the
     *     worker should shut down if the maximum number of concurrent
     *     workers is reduced).
     * @param monitor The <code>ProgressMonitor</code> to wrap.
     */
    public ProgressMonitorWrapper(int workerId, ProgressMonitor monitor) {
      this.workerId = workerId;
      this.monitor = monitor;
      monitor.addCancelListener(cancelListeners);
    }

    /**
     * Waits until the <code>CourtesyMonitor</code> says its okay to
     * proceed.
     */
    private void waitForCourtesyMonitor() {
      if (!courtesyMonitor.allowTasksToRun()) {
        monitor.notifyStatusChanged("Suspended");
        do {
          try {
            courtesyMonitor.waitFor();
          } catch (InterruptedException e) {}
        } while (!courtesyMonitor.allowTasksToRun());
        monitor.notifyStatusChanged("Resumed");
      }
    }

    /**
     * Resets the local cancel pending flag.
     */
    public void reset() {
      cancelPending = false;
    }

    /**
     * Requests that the task being processed be canceled.
     */
    public void cancel() {
      cancelPending = true;
      cancelListeners.cancelRequested();
    }

    /**
     * Determines if the worker is to be shut down.
     * @return A value indicating whether the worker is to be shut down.
     */
    public boolean isWorkerShutdownPending() {
      return shutdownPending || (workerId >= maxWorkers);
    }

    @Override
    public boolean isCancelPending() {
      return isLocalCancelPending() || monitor.isCancelPending();
    }

    @Override
    public void addCancelListener(CancelListener listener) {
      cancelListeners.addCancelListener(listener);
    }

    /**
     * Determines if cancellation is pending due to a request directly to
     * this object that the worker be canceled, or due to the owning worker
     * being shut down (i.e., not due to the decorated
     * <code>ProgressMonitor</code> requesting cancellation.
     * @return A value whether cancellation is pending locally.
     */
    private boolean isLocalCancelPending() {
      return cancelPending || isWorkerShutdownPending();
    }

    @Override
    public void notifyCancelled() {
      if (isWorkerShutdownPending()) {
        monitor.notifyCancelled();
      }
    }

    @Override
    public void notifyComplete() {
      if (isWorkerShutdownPending()) {
        monitor.notifyComplete();
      }
    }

    @Override
    public boolean notifyIndeterminantProgress() {
      waitForCourtesyMonitor();
      return monitor.notifyIndeterminantProgress()
          && !isLocalCancelPending();
    }

    @Override
    public boolean notifyProgress(int value, int maximum) {
      waitForCourtesyMonitor();
      return monitor.notifyProgress(value, maximum)
        && !isLocalCancelPending();
    }

    @Override
    public boolean notifyProgress(double progress) {
      waitForCourtesyMonitor();
      return monitor.notifyProgress(progress)
        && !isLocalCancelPending();
    }

    @Override
    public void notifyStatusChanged(String status) {
      waitForCourtesyMonitor();
      monitor.notifyStatusChanged(status);
    }

  }

  /** The <code>Logger</code> to write log messages to. */
  private static final Logger logger = Logger.getLogger(ThreadServiceWorker.class);

  /** Default message to display while idling. */
  private static final String DEFAULT_IDLE_MESSAGE = "Idling...";

  /** Message to display while idling because an exception was thrown. */
  private static final String EXCEPTION_IDLE_MESSAGE = "Exception thrown, idling...";

  /** Number of seconds to idle after an exception. */
  private static int EXCEPTION_IDLE_SECONDS = 10;

  /** The <code>Executor</code> to use to process tasks. */
  private final Executor executor;

  /**
   * The <code>ProgressMonitorFactory</code> to use to create
   * <code>ProgressMonitor</code>s for worker tasks.
   */
  private final ProgressMonitorFactory monitorFactory;

  /**
   * The <code>CourtesyMonitor</code> to use to determine if we should be
   * allowed to run tasks.
   */
  private final CourtesyMonitor courtesyMonitor;

  /**
   * The <code>JobService</code> to obtain tasks from and submit
   * results to.
   */
  private final ReconnectingJobService service;

  /**
   * The <code>Thread</code> that is currently executing the {@link #run()}
   * method.
   */
  private Thread runThread = null;

  /** A value indicating if thread is about to be shut down. */
  private boolean shutdownPending = false;

  /** The maximum number of workers that may be executing simultaneously. */
  private int maxWorkers;

  /** The number of currently active workers. */
  private int numWorkers;

  /** A queue containing the available workers. */
  private final BlockingQueue<Worker> workerQueue = new LinkedBlockingQueue<Worker>();

  /**
   * A <code>DataSource</code> to use to store cached class definitions.
   */
  private DataSource dataSource = null;

  /**
   * A <code>Lock</code> for controlling access to critical sections for
   * idle polling.
   */
  private final Lock idleLock = new ReentrantLock();

  /**
   * A <code>Condition</code> that is signaled when idling is complete
   * (i.e., when the server has tasks to perform).
   */
  private final Condition idleComplete = idleLock.newCondition();

  /**
   * A flag indicating whether the server is currently serving idle tasks.
   */
  private boolean idling = false;

  /**
   * The ID of the worker designated to poll the server for tasks when
   * idling.
   */
  private int poller = -1;

  /**
   * The <code>Set</code> of <code>Worker</code>s that are currently
   * processing tasks.  This is used by the thread responsible for polling
   * the server to determine if any of those tasks are already complete.
   */
  private final Set<Worker> activeWorkers = Collections.synchronizedSet(new HashSet<Worker>());

  /**
   * The interval (in milliseconds) between requests to the server to obtain
   * a list of completed tasks that this <code>ThreadServiceWorker</code> is
   * processing.
   */
  private final long finishedTaskPollingInterval = 10000;

}
