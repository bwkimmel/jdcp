package ca.eandb.jdcp.job;

import java.io.ObjectInput;
import java.io.ObjectOutput;

public interface LifeCycleManageable {

  /**
   * Sets up this <code>ParallelizableJob</code> on the machine hosting the
   * job (not necessarily the same machine as the one processing the tasks
   * for this job).
   * @throws Exception If an error occurs performing the operation.
   */
  public abstract void initialize() throws Exception;

  /**
   * Performs any final actions required for this
   * <code>ParallelizableJob</code>.
   * @throws Exception If an error occurs performing the operation.
   */
  public abstract void finish() throws Exception;

  /**
   * Notifies the job that it is about to be suspended (for example, if the
   * server application is about to be shut down).
   * @param output The <code>ObjectOutput</code> to save state to.
   * @throws Exception If an error occurs performing the operation.
   */
  public abstract void saveState(ObjectOutput output) throws Exception;

  /**
   * Notifies the job that it is about to be resumed after having been
   * suspended.  The job should reinstate any transient data (e.g., open
   * files).
   * @param input The <code>ObjectInput</code> to restore state from.
   * @throws Exception If an error occurs performing the operation.
   */
  public abstract void restoreState(ObjectInput input) throws Exception;

}