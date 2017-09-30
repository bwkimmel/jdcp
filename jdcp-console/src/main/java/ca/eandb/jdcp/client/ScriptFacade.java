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

package ca.eandb.jdcp.client;

import java.util.UUID;


import ca.eandb.jdcp.job.ParallelizableJob;
import ca.eandb.util.rmi.Serialized;

/**
 * An object provided to scripts to allow them to interface with JDCP.
 * @see ca.eandb.jdcp.client.ScriptCommand
 * @author Brad Kimmel
 */
public final class ScriptFacade {

  /** The application command line options. */
  private final Configuration config;

  /**
   * Creates a new <code>ScriptFacade</code>.
   * @param config The application command line options.
   */
  public ScriptFacade(Configuration config) {
    this.config = config;
  }

  /**
   * Sets the time (in seconds) that the JDCP server will instruct workers to
   * idle if there are no jobs to be processed.
   * @param seconds The idle time (in seconds).
   * @throws Exception if an error occurs in delegating the request to the
   *     configured job service
   */
  public void setIdleTime(int seconds) throws Exception {
    config.getJobService().setIdleTime(seconds);
  }

  /**
   * Sets the priority of the specified job.
   * @param jobId The <code>UUID</code> of the job for which to set the
   *     priority.
   * @param priority The priority to assign to the job.
   * @throws Exception if an error occurs in delegating the request to the
   *     configured job service
   */
  public void setJobPriority(UUID jobId, int priority) throws Exception {
    config.getJobService().setJobPriority(jobId, priority);
  }

  /**
   * Cancel the specified job.
   * @param jobId The <code>UUID</code> identifying the job to cancel.
   * @throws Exception if an error occurs in delegating the request to the
   *     configured job service
   */
  public void cancelJob(UUID jobId) throws Exception {
    config.getJobService().cancelJob(jobId);
  }

  /**
   * Cancel the specified job.
   * @param jobId A <code>String</code> representation of the
   *     <code>UUID</code> identifying the job to cancel.
   * @throws Exception if an error occurs in delegating the request to the
   *     configured job service
   */
  public void cancelJob(String jobId) throws Exception {
    cancelJob(UUID.fromString(jobId));
  }

  /**
   * Submits a job to be processed.
   * @param job The <code>ParallelizableJob</code> to submit.
   * @return The <code>UUID</code> identifying the submitted job.
   * @throws Exception if an error occurs in delegating the request to the
   *     configured job service
   */
  public UUID submitJob(ParallelizableJob job) throws Exception {
    return submitJob(job, job.getClass().getSimpleName());
  }

  /**
   * Submits a job to be processed.
   * @param job The <code>ParallelizableJob</code> to submit.
   * @param description A description of the job.
   * @return The <code>UUID</code> identifying the submitted job.
   * @throws Exception if an error occurs in delegating the request to the
   *     configured job service
   */
  public UUID submitJob(ParallelizableJob job, String description) throws Exception {
    return config.getJobService().submitJob(
        new Serialized<ParallelizableJob>(job), description);
  }

}
