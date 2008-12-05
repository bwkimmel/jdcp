/**
 *
 */
package ca.eandb.jdcp.job;

import ca.eandb.util.progress.ProgressMonitor;

/**
 * A <code>ParallelizableJob</code> decorator that wraps exceptions thrown by
 * the inner <code>ParallelizableJob</code> in a
 * <code>JobExecutionException</code>.
 * @author brad
 * @see ca.eandb.jdcp.job.ParallelizableJob
 * @see ca.eandb.jdcp.job.JobExecutionException
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
	 * @see ca.eandb.jdcp.job.ParallelizableJob#setHostService(ca.eandb.jdcp.job.HostService)
	 */
	@Override
	public void setHostService(HostService host) {
		job.setHostService(host);
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#finish()
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
	 * @see ca.eandb.jdcp.job.ParallelizableJob#getNextTask()
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
	 * @see ca.eandb.jdcp.job.ParallelizableJob#initialize(ca.eandb.jdcp.job.Host)
	 */
	@Override
	public void initialize() throws JobExecutionException {
		try {
			job.initialize();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#suspend()
	 */
	@Override
	public void suspend() throws JobExecutionException {
		try {
			job.suspend();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#resume()
	 */
	@Override
	public void resume() throws JobExecutionException {
		try {
			job.resume();
		} catch (Exception e) {
			throw new JobExecutionException(e);
		}
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.job.ParallelizableJob#isComplete()
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
	 * @see ca.eandb.jdcp.job.ParallelizableJob#submitTaskResults(java.lang.Object, java.lang.Object, ca.eandb.util.progress.ProgressMonitor)
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
	 * @see ca.eandb.jdcp.job.ParallelizableJob#worker()
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
	 * @see ca.eandb.util.jobs.Job#go(ca.eandb.util.progress.ProgressMonitor)
	 */
	@Override
	public boolean go(ProgressMonitor monitor) {
		return job.go(monitor);
	}

}
