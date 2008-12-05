/**
 *
 */
package org.jdcp.server;

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
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.apache.log4j.Logger;
import org.jdcp.job.HostService;
import org.jdcp.job.JobExecutionException;
import org.jdcp.job.JobExecutionWrapper;
import org.jdcp.job.ParallelizableJob;
import org.jdcp.job.TaskDescription;
import org.jdcp.job.TaskWorker;
import org.jdcp.remote.JobService;
import org.jdcp.scheduling.TaskScheduler;
import org.jdcp.server.classmanager.ClassManager;
import org.jdcp.server.classmanager.ParentClassManager;

import ca.eandb.util.io.FileUtil;
import ca.eandb.util.progress.ProgressMonitor;
import ca.eandb.util.rmi.Serialized;
import ca.eandb.util.util.UnexpectedException;
import ca.eandb.util.util.classloader.StrategyClassLoader;

/**
 * @author brad
 *
 */
public final class JobServer implements JobService {

	private static final int DEFAULT_IDLE_SECONDS = 10;

	private static final Logger logger = Logger.getLogger(JobServer.class);

	private final ProgressMonitor monitor;

	private final TaskScheduler scheduler;

	private final ParentClassManager classManager;

	private final File outputDirectory;

	private final Map<UUID, ScheduledJob> jobs = new HashMap<UUID, ScheduledJob>();

	private final Executor executor;

	private TaskDescription idleTask = new TaskDescription(null, 0, DEFAULT_IDLE_SECONDS);

	/**
	 * Creates a new <code>JobServer</code>.
	 * @param outputDirectory The directory to write job results to.
	 * @param monitor The <code>ProgressMonitor</code> to report to.
	 * @param scheduler The <code>TaskScheduler</code> to use to assign
	 * 		tasks.
	 * @param classManager The <code>ParentClassManager</code> to use to
	 * 		store and retrieve class definitions.
	 * @param executor The <code>Executor</code> to use to run bits of code
	 * 		that should not hold up the remote caller.
	 */
	public JobServer(File outputDirectory, ProgressMonitor monitor, TaskScheduler scheduler, ParentClassManager classManager, Executor executor) throws IllegalArgumentException {
		if (!outputDirectory.isDirectory()) {
			throw new IllegalArgumentException("outputDirectory must be a directory.");
		}
		this.outputDirectory = outputDirectory;
		this.monitor = monitor;
		this.scheduler = scheduler;
		this.classManager = classManager;
		this.executor = executor;

		logger.info("JobServer created");
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#createJob(java.lang.String)
	 */
	public UUID createJob(String description) throws SecurityException {
		ScheduledJob sched = new ScheduledJob(description, monitor);
		jobs.put(sched.id, sched);

		if (logger.isInfoEnabled()) {
			logger.info("Job created (" + sched.id.toString() + "): " + description);
		}

		return sched.id;
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Envelope, java.util.UUID)
	 */
	public void submitJob(Serialized<ParallelizableJob> job, UUID jobId)
			throws IllegalArgumentException, SecurityException, ClassNotFoundException, JobExecutionException {
		ScheduledJob sched = jobs.get(jobId);
		if (sched == null || sched.job != null) {
			throw new IllegalArgumentException("No pending job with provided Job ID");
		}

		try {
			sched.initializeJob(job);
			sched.scheduleNextTask();
		} catch (JobExecutionException e) {
			handleJobExecutionException(e, jobId);
			throw e;
		}

		if (logger.isInfoEnabled()) {
			logger.info("Pending job submitted (" + jobId.toString() + ")");
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#submitJob(ca.eandb.util.rmi.Envelope, java.lang.String)
	 */
	public UUID submitJob(Serialized<ParallelizableJob> job, String description)
			throws SecurityException, ClassNotFoundException, JobExecutionException {
		ScheduledJob sched = new ScheduledJob(description, monitor);
		jobs.put(sched.id, sched);

		try {
			sched.initializeJob(job);
			sched.scheduleNextTask();
		} catch (JobExecutionException e) {
			handleJobExecutionException(e, sched.id);
			throw e;
		}

		if (logger.isInfoEnabled()) {
			logger.info("Job submitted (" + sched.id.toString() + "): "
					+ description);
		}

		return sched.id;
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#cancelJob(java.util.UUID)
	 */
	public void cancelJob(UUID jobId) throws IllegalArgumentException, SecurityException {
		if (!jobs.containsKey(jobId)) {
			throw new IllegalArgumentException("No job with provided Job ID");
		}

		removeScheduledJob(jobId, false);
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#getTaskWorker(java.util.UUID)
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
	 * @see org.jdcp.remote.JobService#requestTask()
	 */
	public synchronized TaskDescription requestTask() throws SecurityException {
		TaskDescription taskDesc = scheduler.getNextTask();
		if (taskDesc == null) {
			return idleTask;
		}

		ScheduledJob sched = jobs.get(taskDesc.getJobId());
		try {
			sched.scheduleNextTask();
		} catch (JobExecutionException e) {
			handleJobExecutionException(e, sched.id);
		}
		return taskDesc;
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#submitTaskResults(java.util.UUID, int, ca.eandb.util.rmi.Envelope)
	 */
	public void submitTaskResults(final UUID jobId, final int taskId,
			final Serialized<Object> results) throws SecurityException {
		ScheduledJob sched = jobs.get(jobId);
		if (sched != null) {
			sched.submitTaskResults(taskId, results);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#reportException(java.util.UUID, int, java.lang.Exception)
	 */
	public void reportException(UUID jobId, int taskId, Exception e)
			throws SecurityException, RemoteException {
		ScheduledJob sched = jobs.get(jobId);
		if (sched != null) {
			sched.reportException(taskId, e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#getClassDefinition(java.lang.String, java.util.UUID)
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
	 * @see org.jdcp.remote.JobService#getClassDigest(java.lang.String, java.util.UUID)
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
	 * @see org.jdcp.remote.JobService#getClassDigest(java.lang.String)
	 */
	public byte[] getClassDigest(String name) throws SecurityException {
		return classManager.getClassDigest(name);
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#setClassDefinition(java.lang.String, byte[])
	 */
	public void setClassDefinition(String name, byte[] def)
			throws SecurityException {
		classManager.setClassDefinition(name, def);

		if (logger.isInfoEnabled()) {
			logger.info("Global class definition updated for " + name);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#setClassDefinition(java.lang.String, java.util.UUID, byte[])
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
	 * @see org.jdcp.remote.JobService#setIdleTime(int)
	 */
	public void setIdleTime(int idleSeconds) throws IllegalArgumentException,
			SecurityException {
		idleTask = new TaskDescription(null, 0, idleSeconds);
		if (logger.isInfoEnabled()) {
			logger.info("Idle time set to " + Integer.toString(idleSeconds));
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.remote.JobService#setJobPriority(java.util.UUID, int)
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

	private void handleJobExecutionException(JobExecutionException e, UUID jobId) {
		logger.error("Exception thrown from job " + jobId.toString(), e);
		removeScheduledJob(jobId, false);
	}

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
			classManager.releaseChildClassManager(sched.classManager);
		}
	}

	/**
	 * Represents a <code>ParallelizableJob</code> that has been submitted
	 * to this <code>JobMasterServer</code>.
	 * @author bkimmel
	 */
	private class ScheduledJob implements HostService {

		/** The <code>ParallelizableJob</code> to be processed. */
		public JobExecutionWrapper				job;

		/** The <code>UUID</code> identifying the job. */
		public final UUID						id;

		/** A description of the job. */
		public final String						description;

		/** The <code>TaskWorker</code> to use to process tasks for the job. */
		public Serialized<TaskWorker>				worker;

		/**
		 * The <code>ProgressMonitor</code> to use to monitor the progress of
		 * the <code>Job</code>.
		 */
		public final ProgressMonitor			monitor;

		/**
		 * The <code>ClassManager</code> to use to store the class definitions
		 * applicable to this job.
		 */
		public final ClassManager				classManager;

		private final File						workingDirectory;

		/**
		 * Initializes the scheduled job.
		 * @param description A description of the job.
		 * @param monitor The <code>ProgressMonitor</code> from which to create a child
		 * 		monitor to use to monitor the progress of the
		 * 		<code>ParallelizableJob</code>.
		 */
		public ScheduledJob(String description, ProgressMonitor monitor) {

			this.id					= UUID.randomUUID();
			this.description		= description;

			//String title			= String.format("%s (%s)", this.job.getClass().getSimpleName(), this.id.toString());
			this.monitor			= monitor.createChildProgressMonitor(description);
			this.monitor.notifyStatusChanged("Awaiting job submission");

			this.classManager		= JobServer.this.classManager.createChildClassManager();

			this.workingDirectory	= new File(outputDirectory, id.toString());

		}

		public void initializeJob(Serialized<ParallelizableJob> job) throws ClassNotFoundException, JobExecutionException {
			ClassLoader loader	= new StrategyClassLoader(classManager, JobServer.class.getClassLoader());
			this.job			= new JobExecutionWrapper(job.deserialize(loader));
			this.worker			= new Serialized<TaskWorker>(this.job.worker());
			this.monitor.notifyStatusChanged("");

			this.workingDirectory.mkdir();
			this.job.setHostService(this);
			this.job.initialize();
		}

		public void submitTaskResults(int taskId, Serialized<Object> results) {
			Object task = scheduler.remove(id, taskId);
			Runnable command = new TaskResultSubmitter(this, task, results, monitor);
			try {
				executor.execute(command);
			} catch (RejectedExecutionException e) {
				command.run();
			}
		}

		public synchronized void reportException(int taskId, Exception ex)
				throws SecurityException, RemoteException {

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

		public void scheduleNextTask() throws JobExecutionException {
			Object task = job.getNextTask();
			if (task != null) {
				scheduler.add(id, task);
			}
		}

		/**
		 * Writes the results of a <code>ScheduledJob</code> to the output
		 * directory.
		 * @param sched The <code>ScheduledJob</code> to write results for.
		 * @throws JobExecutionException
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

	}

	private class TaskResultSubmitter implements Runnable {

		private final ScheduledJob sched;
		private final Object task;
		private final Serialized<Object> results;
		private final ProgressMonitor monitor;

		/**
		 * @param sched
		 * @param task
		 * @param results
		 * @param monitor
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
		@Override
		public void run() {
			ClassLoader cl = sched.job.getClass().getClassLoader();
			if (task != null) {
				try {
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
				}
			}
		}

	}

}
