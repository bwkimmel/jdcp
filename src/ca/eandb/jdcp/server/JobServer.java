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
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;

import ca.eandb.jdcp.job.HostService;
import ca.eandb.jdcp.job.JobExecutionException;
import ca.eandb.jdcp.job.JobExecutionWrapper;
import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.jdcp.job.TaskDescription;
import ca.eandb.jdcp.job.TaskWorker;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.server.classmanager.ChildClassManager;
import ca.eandb.jdcp.server.classmanager.ParentClassManager;
import ca.eandb.jdcp.server.scheduling.TaskScheduler;
import ca.eandb.util.UnexpectedException;
import ca.eandb.util.classloader.StrategyClassLoader;
import ca.eandb.util.io.FileUtil;
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

	/** The <code>Random</code> number generator (for generating task IDs. */
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
	private final Map<UUID, ScheduledJob> jobs = new HashMap<UUID, ScheduledJob>();

	/** An <code>Executor</code> to use to run asynchronous tasks. */
	private final Executor executor;

	/**
	 * The <code>TaskDescription</code> to use to notify workers that no tasks
	 * are available to be performed.
	 */
	private TaskDescription idleTask = new TaskDescription(null, 0, DEFAULT_IDLE_SECONDS);

	/**
	 * Creates a new <code>JobServer</code>.
	 * @param outputDirectory The directory to write job results to.
	 * @param monitorFactory The <code>ProgressMonitorFactory</code> to use to
	 * 		create <code>ProgressMonitor</code>s for individual jobs.
	 * @param scheduler The <code>TaskScheduler</code> to use to assign
	 * 		tasks.
	 * @param classManager The <code>ParentClassManager</code> to use to
	 * 		store and retrieve class definitions.
	 * @param executor The <code>Executor</code> to use to run bits of code
	 * 		that should not hold up the remote caller.
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

		logger.info("JobServer created");
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#createJob(java.lang.String)
	 */
	public UUID createJob(String description) throws SecurityException {
		ScheduledJob sched = new ScheduledJob(description, monitorFactory.createProgressMonitor(description));
		jobs.put(sched.id, sched);

		if (logger.isInfoEnabled()) {
			logger.info("Job created (" + sched.id.toString() + "): " + description);
		}

		return sched.id;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Envelope, java.util.UUID)
	 */
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

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Envelope, java.lang.String)
	 */
	public UUID submitJob(Serialized<ParallelizableJob> job, String description)
			throws SecurityException, ClassNotFoundException, JobExecutionException {
		ScheduledJob sched = new ScheduledJob(description, monitorFactory.createProgressMonitor(description));
		jobs.put(sched.id, sched);

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

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#cancelJob(java.util.UUID)
	 */
	public void cancelJob(UUID jobId) throws IllegalArgumentException, SecurityException {
		if (!jobs.containsKey(jobId)) {
			throw new IllegalArgumentException("No job with provided Job ID");
		}

		removeScheduledJob(jobId, false);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getTaskWorker(java.util.UUID)
	 */
	public Serialized<TaskWorker> getTaskWorker(UUID jobId)
			throws IllegalArgumentException, SecurityException {
		ScheduledJob sched = jobs.get(jobId);
		if (sched == null) {
			throw new IllegalArgumentException("No submitted job with provided Job ID");
		}

		return sched.worker;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#requestTask()
	 */
	public synchronized TaskDescription requestTask() throws SecurityException {
		TaskDescription taskDesc = scheduler.getNextTask();
		if (taskDesc == null) {
			return idleTask;
		}

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

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Envelope)
	 */
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
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#reportException(java.util.UUID, int, java.lang.Exception)
	 */
	public void reportException(UUID jobId, int taskId, Exception e)
			throws SecurityException, RemoteException {
		ScheduledJob sched = jobs.get(jobId);
		if (sched != null) {
			sched.reportException(taskId, e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDefinition(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDefinition(String name, UUID jobId)
			throws SecurityException {
		ScheduledJob sched = jobs.get(jobId);
		if (sched == null) {
			throw new IllegalArgumentException("No job with provided Job ID");
		}

		ByteBuffer def = sched.classManager.getClassDefinition(name);
		if (def.hasArray() && def.arrayOffset() == 0) {
			return def.array();
		} else {
			byte[] bytes = new byte[def.remaining()];
			def.get(bytes);
			return bytes;
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String, java.util.UUID)
	 */
	public byte[] getClassDigest(String name, UUID jobId)
			throws SecurityException {
		ScheduledJob sched = jobs.get(jobId);
		if (sched == null) {
			throw new IllegalArgumentException("No job with provided Job ID");
		}

		return sched.classManager.getClassDigest(name);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name) throws SecurityException {
		return classManager.getClassDigest(name);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, byte[])
	 */
	public void setClassDefinition(String name, byte[] def)
			throws SecurityException {
		classManager.setClassDefinition(name, def);

		if (logger.isInfoEnabled()) {
			logger.info("Global class definition updated for " + name);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setClassDefinition(java.lang.String, java.util.UUID, byte[])
	 */
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

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setIdleTime(int)
	 */
	public void setIdleTime(int idleSeconds) throws IllegalArgumentException,
			SecurityException {
		idleTask = new TaskDescription(null, 0, idleSeconds);
		if (logger.isInfoEnabled()) {
			logger.info("Idle time set to " + Integer.toString(idleSeconds));
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.remote.JobService#setJobPriority(java.util.UUID, int)
	 */
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
	 * 		job.
	 * @param jobId The <code>UUID</code> identifying the job that threw the
	 * 		exception.
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
				sched.monitor.notifyComplete();
				if (logger.isInfoEnabled()) {
					logger.info("Job complete (" + jobId.toString() + ")");
				}
			} else {
				sched.monitor.notifyCancelled();
				if (logger.isInfoEnabled()) {
					logger.info("Job cancelled (" + jobId.toString() + ")");
				}
			}
			jobs.remove(jobId);
			scheduler.removeJob(jobId);
			sched.classManager.release();
		}
	}

	/**
	 * Represents a <code>ParallelizableJob</code> that has been submitted
	 * to this <code>JobMasterServer</code>.
	 * @author Brad Kimmel
	 */
	private class ScheduledJob implements HostService {

		/** The <code>ParallelizableJob</code> to be processed. */
		public JobExecutionWrapper				job;

		/** The <code>UUID</code> identifying the job. */
		public final UUID						id;

		/** A description of the job. */
		public final String						description;

		/** The <code>TaskWorker</code> to use to process tasks for the job. */
		public Serialized<TaskWorker>			worker;

		/**
		 * The <code>ProgressMonitor</code> to use to monitor the progress of
		 * the <code>Job</code>.
		 */
		public final ProgressMonitor			monitor;

		/**
		 * The <code>ClassManager</code> to use to store the class definitions
		 * applicable to this job.
		 */
		public final ChildClassManager			classManager;

		/** The working directory for this job. */
		private final File						workingDirectory;

		/** The <code>ClassLoader</code> to use to deserialize this job. */
		public ClassLoader						classLoader;

		/**
		 * Initializes the scheduled job.
		 * @param description A description of the job.
		 * @param monitor The <code>ProgressMonitor</code> to use to monitor
		 * 		the progress of the <code>ParallelizableJob</code>.
		 */
		public ScheduledJob(String description, ProgressMonitor monitor) {

			this.id					= UUID.randomUUID();
			this.description		= description;

			//String title			= String.format("%s (%s)", this.job.getClass().getSimpleName(), this.id.toString());
			this.monitor			= monitor;
			this.monitor.notifyStatusChanged("Awaiting job submission");

			this.classManager		= JobServer.this.classManager.createChildClassManager();

			this.workingDirectory	= new File(outputDirectory, id.toString());

		}

		/**
		 * Deserializes the job and prepares it to be managed by the host
		 * machine.
		 * @param job The serialized job.
		 * @throws ClassNotFoundException If a class required by the job is
		 * 		missing.
		 * @throws JobExecutionException If the job throws an exception.
		 */
		public void initializeJob(Serialized<ParallelizableJob> job) throws ClassNotFoundException, JobExecutionException {
			this.classLoader	= new StrategyClassLoader(classManager, JobServer.class.getClassLoader());
			this.job			= new JobExecutionWrapper(job.deserialize(classLoader));
			this.worker			= new Serialized<TaskWorker>(this.job.worker());
			this.monitor.notifyStatusChanged("");

			this.workingDirectory.mkdir();
			this.job.setHostService(this);

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
				Runnable command = new TaskResultSubmitter(this, task, results, monitor);
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
					log.println("A worker reported an exception while processing the job:");
				} else {
					log.println("A worker reported an exception while processing a task (" + Integer.toString(taskId) + "):");
				}
				log.println(ex);
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
			} while (scheduler.contains(id, taskId));
			return taskId;
		}

		/**
		 * Obtains and schedules the next task for this job.
		 * @throws JobExecutionException If the job throws an exception while
		 * 		attempting to obtain the next task.
		 */
		public void scheduleNextTask() throws JobExecutionException {
			Object task = job.getNextTask();
			if (task != null) {
				int taskId = generateTaskId();
				TaskDescription desc = new TaskDescription(id, taskId, task);
				scheduler.add(desc);
			}
		}

		/**
		 * Writes the results of a <code>ScheduledJob</code> to the output
		 * directory.
		 * @param sched The <code>ScheduledJob</code> to write results for.
		 * @throws JobExecutionException If the job throws an exception.
		 */
		private synchronized void finalizeJob() throws JobExecutionException {

			assert(job.isComplete());

			job.finish();

			try {

				String				filename		= String.format("%s.zip", id.toString());
				File				outputFile		= new File(outputDirectory, filename);

				File				logFile			= new File(workingDirectory, "job.log");
				PrintStream			log				= new PrintStream(new FileOutputStream(logFile, true));

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
		 * 		or references the parent directory (..).
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

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.job.HostService#createFileOutputStream(java.lang.String)
		 */
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

		/* (non-Javadoc)
		 * @see ca.eandb.jdcp.job.HostService#createRandomAccessFile(java.lang.String)
		 */
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
		 * 		whose results are being submitted.
		 * @param task The <code>Object</code> describing the task whose
		 * 		results are being submitted.
		 * @param results The serialized task results.
		 * @param monitor The <code>ProgressMonitor</code> to report job
		 * 		progress to.
		 */
		public TaskResultSubmitter(ScheduledJob sched, Object task,
				Serialized<Object> results, ProgressMonitor monitor) {
			this.sched = sched;
			this.task = task;
			this.results = results;
			this.monitor = monitor;
		}

		/* (non-Javadoc)
		 * @see java.lang.Runnable#run()
		 */
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

}
