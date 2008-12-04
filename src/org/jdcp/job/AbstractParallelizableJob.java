/**
 *
 */
package org.jdcp.job;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import org.selfip.bkimmel.progress.ProgressMonitor;

/**
 * An abstract <code>ParallelizableJob</code> that provides a default
 * implementation for the <code>Job</code> interface.
 * @author bkimmel
 */
public abstract class AbstractParallelizableJob implements ParallelizableJob {

	private transient HostService host = null;

	public final void setHostService(HostService host) {
		this.host = host;
	}

	public void initialize() throws Exception {
		/* nothing to do. */
	}

	public void finish() throws Exception {
		/* nothing to do. */
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#suspend(java.io.OutputStream)
	 */
	public void suspend(OutputStream stream) throws Exception {
		/* nothing to do. */
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#resume(java.io.InputStream)
	 */
	public void resume(InputStream stream) throws Exception {
		/* nothing to do. */
	}

	/**
	 * @param name
	 * @return
	 * @see org.jdcp.job.HostService#createFileOutputStream(java.lang.String)
	 */
	protected FileOutputStream createFileOutputStream(String name) {
		return host.createFileOutputStream(name);
	}

	/**
	 * @param name
	 * @return
	 * @see org.jdcp.job.HostService#createRandomAccessFile(java.lang.String)
	 */
	protected RandomAccessFile createRandomAccessFile(String name) {
		return host.createRandomAccessFile(name);
	}

	/* (non-Javadoc)
	 * @see org.jmist.framework.Job#go(org.jmist.framework.reporting.ProgressMonitor)
	 */
	public boolean go(ProgressMonitor monitor) {

		try {

			/* Get the task worker. */
			TaskWorker		worker			= this.worker();
			int			 	taskNumber		= 0;

			/* Check to see if the process has been cancelled. */
			monitor.notifyIndeterminantProgress();

			/* Main loop. */
			while (!monitor.isCancelPending()) {

				/* Get the next task. */
				Object				task			= this.getNextTask();

				/* If there is no next task, then we're done. */
				if (task == null) {
					monitor.notifyComplete();
					return true;
				}

				/* Create a progress monitor to monitor the task. */
				String				taskDesc		= String.format("Task %d", ++taskNumber);
				ProgressMonitor		taskMonitor		= monitor.createChildProgressMonitor(taskDesc);

				/* Perform the task. */
				Object				results			= worker.performTask(task, taskMonitor);

				/* If the task was cancelled, then cancel the job. */
				if (taskMonitor.isCancelPending()) {
					break;
				}

				/* Submit the task results. */
				this.submitTaskResults(task, results, monitor);

			}

		} catch (Exception e) {

			e.printStackTrace();

		}

		/* If we get to this point, then the job was cancelled. */
		monitor.notifyCancelled();
		return false;

	}

}
