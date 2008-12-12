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

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.jnlp.UnavailableServiceException;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.classloader.ClassLoaderStrategy;
import ca.eandb.util.classloader.StrategyClassLoader;
import ca.eandb.util.progress.ProgressMonitor;
import ca.eandb.util.progress.ProgressMonitorFactory;
import ca.eandb.util.rmi.Serialized;

/**
 * A <code>Runnable</code> worker that processes tasks for a
 * <code>ParallelizableJob</code> from a remote <code>JobServiceMaster<code>.
 * This class may potentially use multiple threads to process tasks.
 * @author Brad Kimmel
 */
public final class ThreadServiceWorker implements Runnable {

	/**
	 * Initializes the address of the master and the amount of time to idle
	 * when no task is available.
	 * @param masterHost The URL of the master.
	 * @param idleTime The time (in seconds) to idle when no task is
	 * 		available.
	 * @param maxConcurrentWorkers The maximum number of concurrent worker
	 * 		threads to allow.
	 * @param executor The <code>Executor</code> to use to process tasks.
	 * @param monitorFactory The <code>ProgressMonitorFactory</code> to use to
	 * 		create <code>ProgressMonitor</code>s for worker tasks.
	 */
	public ThreadServiceWorker(JobServiceFactory serviceFactory, int maxConcurrentWorkers, Executor executor, ProgressMonitorFactory monitorFactory) {

		assert(maxConcurrentWorkers > 0);

		this.serviceFactory = serviceFactory;
		this.executor = executor;
		this.maxConcurrentWorkers = maxConcurrentWorkers;
		this.monitorFactory = monitorFactory;

	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	public synchronized void run() {

		runThread = Thread.currentThread();
		reconnectPending = true;

		this.initializeWorkers(maxConcurrentWorkers);

		while (!shutdownPending) {

			try {
				while (!Thread.interrupted()) {
					Worker worker = this.workerQueue.take();
					if (reconnectPending) {
						this.initializeService();
						reconnectPending = false;
					}
					this.executor.execute(worker);
				}
			} catch (InterruptedException e) {
				/* nothing to do. */
			}

		}

		runThread = null;

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
	 * Schedules a reconnection to the <code>JobService</code>.
	 */
	private void reconnect() {
		reconnectPending = true;
	}

	/**
	 * Initializes the worker queue with the specified number of workers.
	 * @param numWorkers The number of workers to create.
	 * @param parentMonitor The <code>ProgressMonitor</code> to use to create
	 * 		child <code>ProgressMonitor</code>s for each <code>Worker</code>.
	 */
	private void initializeWorkers(int numWorkers) {
		for (int i = 0; i < numWorkers; i++) {
			String title = String.format("Worker (%d)", i + 1);
			ProgressMonitor monitor = new ProgressMonitorWrapper(monitorFactory.createProgressMonitor(title));
			workerQueue.add(new Worker(monitor));
		}
	}

	/**
	 * Attempt to initialize a connection to the master service.
	 * @return A value indicating whether the operation succeeded.
	 */
	private void initializeService() {
		this.service = serviceFactory.connect();
	}

	/**
	 * An entry in the <code>TaskWorker</code> cache.
	 * @author Brad Kimmel
	 */
	private static class WorkerCacheEntry {

		/**
		 * Initializes the cache entry.
		 * @param jobId The <code>UUID</code> of the job that the
		 * 		<code>TaskWorker</code> processes tasks for.
		 */
		public WorkerCacheEntry(UUID jobId) {
			this.jobId = jobId;
			this.workerGuard.writeLock().lock();
		}

		/**
		 * Returns a value indicating if this <code>WorkerCacheEntry</code>
		 * is to be used for the job with the specified <code>UUID</code>.
		 * @param jobId The job's <code>UUID</code> to test.
		 * @return A value indicating if this <code>WorkerCacheEntry</code>
		 * 		applies to the specified job.
		 */
		public boolean matches(UUID jobId) {
			return this.jobId.equals(jobId);
		}

		/**
		 * Sets the <code>TaskWorker</code> to use.  This method may only be
		 * called once.
		 * @param worker The <code>TaskWorker</code> to use for matching
		 * 		jobs.
		 */
		public synchronized void setWorker(TaskWorker worker) {

			/* Set the worker. */
			this.worker = worker;

			/* Release the lock. */
			this.workerGuard.writeLock().unlock();

		}

		/**
		 * Gets the <code>TaskWorker</code> to use to process tasks for the
		 * matching job.  This method will wait for <code>setWorker</code>
		 * to be called if it has not yet been called.
		 * @return The <code>TaskWorker</code> to use to process tasks for
		 * 		the matching job.
		 * @see {@link #setWorker(TaskWorker)}.
		 */
		public TaskWorker getWorker() {

			this.workerGuard.readLock().lock();
			TaskWorker worker = this.worker;
			this.workerGuard.readLock().unlock();

			return worker;

		}

		/**
		 * The <code>UUID</code> of the job that the <code>TaskWorker</code>
		 * processes tasks for.
		 */
		private final UUID jobId;

		/**
		 * The cached <code>TaskWorker</code>.
		 */
		private TaskWorker worker;

		/**
		 * The <code>ReadWriteLock</code> to use before reading from or writing
		 * to the <code>worker</code> field.
		 */
		private final ReadWriteLock workerGuard = new ReentrantReadWriteLock();

	}

	/**
	 * Searches for the <code>WorkerCacheEntry</code> matching the job with the
	 * specified <code>UUID</code>.
	 * @param jobId The <code>UUID</code> of the job whose
	 * 		<code>WorkerCacheEntry</code> to search for.
	 * @return The <code>WorkerCacheEntry</code> corresponding to the job with
	 * 		the specified <code>UUID</code>, or <code>null</code> if the
	 * 		no such entry exists.
	 */
	private WorkerCacheEntry getCacheEntry(UUID jobId) {

		assert(jobId != null);

		synchronized (this.workerCache) {

			Iterator<WorkerCacheEntry> i = this.workerCache.iterator();

			/* Search for the worker for the specified job. */
			while (i.hasNext()) {

				WorkerCacheEntry entry = i.next();

				if (entry.matches(jobId)) {

					/* Remove the entry and re-insert it at the end of the list.
					 * This will ensure that when an item is removed from the list,
					 * the item that is removed will always be the least recently
					 * used.
					 */
					i.remove();
					this.workerCache.add(entry);

					return entry;

				}

			}

			/* cache miss */
			return null;

		}

	}

	/**
	 * Removes the specified entry from the task worker cache.
	 * @param entry The <code>WorkerCacheEntry</code> to remove.
	 */
	private void removeCacheEntry(WorkerCacheEntry entry) {

		assert(entry != null);

		synchronized (this.workerCache) {
			this.workerCache.remove(entry);
		}

	}

	/**
	 * Removes least recently used entries from the task worker cache until
	 * there are at most <code>this.maxCachedWorkers</code> entries.
	 */
	private void removeOldCacheEntries() {

		synchronized (this.workerCache) {

			/* If the cache has exceeded capacity, then remove the least
			 * recently used entry.
			 */
			assert(this.maxCachedWorkers > 0);

			while (this.workerCache.size() > this.maxCachedWorkers) {
				this.workerCache.remove(0);
			}

		}

	}

	/**
	 * Obtains the task worker to process tasks for the job with the specified
	 * <code>UUID</code>.
	 * @param jobId The <code>UUID</code> of the job to obtain the task worker
	 * 		for.
	 * @return The <code>TaskWorker</code> to process tasks for the job with
	 * 		the specified <code>UUID</code>, or <code>null</code> if the job
	 * 		is invalid or has already been completed.
	 * @throws RemoteException
	 * @throws ClassNotFoundException
	 */
	private TaskWorker getTaskWorker(UUID jobId) throws RemoteException, ClassNotFoundException {

		WorkerCacheEntry entry = null;
		boolean hit;

		synchronized (this.workerCache) {

			/* First try to get the worker from the cache. */
			entry = this.getCacheEntry(jobId);
			hit = (entry != null);

			/* If there was no matching cache entry, then add a new entry to
			 * the cache.
			 */
			if (!hit) {
				entry = new WorkerCacheEntry(jobId);
				this.workerCache.add(entry);
			}

		}

		if (hit) {

			/* We found a cache entry, so get the worker from that entry. */
			return entry.getWorker();

		} else { /* cache miss */

			/* The task worker was not in the cache, so use the service to
			 * obtain the task worker.
			 */
			Serialized<TaskWorker> envelope = this.service.getTaskWorker(jobId);

			ClassLoaderStrategy strategy;
			try {
				strategy = new PersistenceCachingJobServiceClassLoaderStrategy(service, jobId);
			} catch (UnavailableServiceException e) {
				strategy = new FileCachingJobServiceClassLoaderStrategy(service, jobId, "./worker");
			}

			ClassLoader loader = new StrategyClassLoader(strategy, ThreadServiceWorker.class.getClassLoader());
			TaskWorker worker = envelope.deserialize(loader);
			entry.setWorker(worker);

			/* If we couldn't get a worker from the service, then don't keep
			 * the cache entry.
			 */
			if (worker == null) {
				this.removeCacheEntry(entry);
			}

			/* Clean up the cache. */
			this.removeOldCacheEntries();

			return worker;

		}

	}

	/**
	 * Used to process tasks in threads.
	 * @author Brad Kimmel
	 */
	private class Worker implements Runnable {

		/**
		 * Initializes the progress monitor to report to.
		 * @param monitor The <code>ProgressMonitor</code> to report
		 * 		the progress of the task to.
		 */
		public Worker(ProgressMonitor monitor) {
			this.monitor = monitor;
		}

		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
		public void run() {

			try {

				this.monitor.notifyIndeterminantProgress();
				this.monitor.notifyStatusChanged("Requesting task...");

				if (service != null) {

					TaskDescription taskDesc = service.requestTask();
					UUID jobId = taskDesc.getJobId();

					if (jobId != null) {

						this.monitor.notifyStatusChanged("Obtaining task worker...");
						TaskWorker worker;
						try {
							worker = getTaskWorker(jobId);
						} catch (ClassNotFoundException e) {
							service.reportException(jobId, 0, e);
							worker = null;
						}

						if (worker == null) {
							this.monitor.notifyStatusChanged("Could not obtain worker...");
							this.monitor.notifyCancelled();
							return;
						}

						this.monitor.notifyStatusChanged("Performing task...");
						ClassLoader loader = worker.getClass().getClassLoader();
						Object results;

						try {
							Object task = taskDesc.getTask().deserialize(loader);
							results = worker.performTask(task, monitor);
						} catch (Exception e) {
							service.reportException(jobId, taskDesc.getTaskId(), e);
							results = null;
						}

						this.monitor.notifyStatusChanged("Submitting task results...");
						if (results != null) {
							service.submitTaskResults(jobId, taskDesc.getTaskId(), new Serialized<Object>(results));
						}

					} else {

						try {
							int seconds = (Integer) taskDesc.getTask().deserialize();
							this.idle(seconds);
						} catch (ClassNotFoundException e) {
							throw new UnexpectedException(e);
						}

					}

					this.monitor.notifyComplete();

				}

			} catch (RemoteException e) {

				logger.error("Could not communicate with master.", e);

				this.monitor.notifyStatusChanged("Failed to communicate with master.");
				reconnect();

				this.monitor.notifyCancelled();

			} finally {

				workerQueue.add(this);

			}

		}

		/**
		 * Idles for the specified number of seconds.
		 * @param seconds The number of seconds to idle for.
		 */
		private void idle(int seconds) {

			monitor.notifyStatusChanged("Idling...");

			for (int i = 0; i < seconds; i++) {

				if (!monitor.notifyProgress(i, seconds)) {
					monitor.notifyCancelled();
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
		private final ProgressMonitor monitor;

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
		 * Creates a new <code>ProgressMonitorWrapper</code>.
		 * @param monitor The <code>ProgressMonitor</code> to wrap.
		 */
		public ProgressMonitorWrapper(ProgressMonitor monitor) {
			this.monitor = monitor;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.progress.ProgressMonitor#isCancelPending()
		 */
		@Override
		public boolean isCancelPending() {
			return shutdownPending;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.progress.ProgressMonitor#notifyCancelled()
		 */
		@Override
		public void notifyCancelled() {
			/* ignore. */
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.progress.ProgressMonitor#notifyComplete()
		 */
		@Override
		public void notifyComplete() {
			/* ignore. */
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.progress.ProgressMonitor#notifyIndeterminantProgress()
		 */
		@Override
		public boolean notifyIndeterminantProgress() {
			monitor.notifyIndeterminantProgress();
			return !shutdownPending;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.progress.ProgressMonitor#notifyProgress(int, int)
		 */
		@Override
		public boolean notifyProgress(int value, int maximum) {
			monitor.notifyProgress(value, maximum);
			return !shutdownPending;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.progress.ProgressMonitor#notifyProgress(double)
		 */
		@Override
		public boolean notifyProgress(double progress) {
			monitor.notifyProgress(progress);
			return !shutdownPending;
		}

		/* (non-Javadoc)
		 * @see ca.eandb.util.progress.ProgressMonitor#notifyStatusChanged(java.lang.String)
		 */
		@Override
		public void notifyStatusChanged(String status) {
			monitor.notifyStatusChanged(status);
		}

	}

	/** The <code>Logger</code> to write log messages to. */
	private static final Logger logger = Logger.getLogger(ThreadServiceWorker.class);

	private final JobServiceFactory serviceFactory;

	/** The <code>Executor</code> to use to process tasks. */
	private final Executor executor;

	/**
	 * The <code>ProgressMonitorFactory</code> to use to create
	 * <code>ProgressMonitor</code>s for worker tasks.
	 */
	private final ProgressMonitorFactory monitorFactory;

	/**
	 * The <code>JobService</code> to obtain tasks from and submit
	 * results to.
	 */
	private JobService service = null;

	/**
	 * The <code>Thread</code> that is currently executing the {@link #run()}
	 * method.
	 */
	private Thread runThread = null;

	/** A value indicating if thread is about to be shut down. */
	private boolean shutdownPending = false;

	/** A value indicating if the worker should reconnect. */
	private boolean reconnectPending;

	/** The maximum number of workers that may be executing simultaneously. */
	private final int maxConcurrentWorkers;

	/** A queue containing the available workers. */
	private final BlockingQueue<Worker> workerQueue = new LinkedBlockingQueue<Worker>();

	/**
	 * A list of recently used <code>TaskWorker</code>s and their associated
	 * job's <code>UUID</code>s, in order from least recently used to most
	 * recently used.
	 */
	private final List<WorkerCacheEntry> workerCache = new LinkedList<WorkerCacheEntry>();

	/**
	 * The maximum number of <code>TaskWorker</code>s to retain in the cache.
	 */
	private final int maxCachedWorkers = 5;

}
