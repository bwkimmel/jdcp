/**
 *
 */
package org.jdcp.job;

import java.io.InputStream;
import java.io.OutputStream;

import org.selfip.bkimmel.progress.ProgressMonitor;

/**
 * A <code>ParallelizableJob</code> decorator that wraps exceptions thrown by
 * the inner <code>ParallelizableJob</code> in a
 * <code>JobExecutionException</code>.
 * @author brad
 * @see org.jdcp.job.ParallelizableJob
 * @see org.jdcp.job.JobExecutionException
 */
public final class JobExecutionWrapper implements ParallelizableJob {

	/** The inner <code>ParallelizableJob</code>. */
	private final ParallelizableJob job;

	/**
	 * Creates a new <code>JobExecutionWrapper</code>.
	 * @param job The inner <code>ParallelizableJob</code>.
	 */
	public JobExecutionWrapper(ParallelizableJob job) {
		this.job = job;
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#finish()
	 */
	@Override
	public void finish() throws JobExecutionException {
		try {
			job.finish();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#getNextTask()
	 */
	@Override
	public Object getNextTask() throws JobExecutionException {
		try {
			return job.getNextTask();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#initialize(org.jdcp.job.Host)
	 */
	@Override
	public void initialize(Host host) throws JobExecutionException {
		try {
			job.initialize(host);
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#suspend(java.io.OutputStream)
	 */
	@Override
	public void suspend(OutputStream stream) throws JobExecutionException {
		try {
			job.suspend(stream);
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#resume(java.io.InputStream)
	 */
	@Override
	public void resume(InputStream stream) throws JobExecutionException {
		try {
			job.resume(stream);
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#isComplete()
	 */
	@Override
	public boolean isComplete() throws JobExecutionException {
		try {
			return job.isComplete();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#submitTaskResults(java.lang.Object, java.lang.Object, org.selfip.bkimmel.progress.ProgressMonitor)
	 */
	@Override
	public void submitTaskResults(Object task, Object results,
			ProgressMonitor monitor) throws JobExecutionException {
		try {
			job.submitTaskResults(task, results, monitor);
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.jdcp.job.ParallelizableJob#worker()
	 */
	@Override
	public TaskWorker worker() throws JobExecutionException {
		try {
			return job.worker();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see org.selfip.bkimmel.jobs.Job#go(org.selfip.bkimmel.progress.ProgressMonitor)
	 */
	@Override
	public boolean go(ProgressMonitor monitor) {
		return job.go(monitor);
	}

}
