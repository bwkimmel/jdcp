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

package ca.eandb.jdcp.remote;

import java.io.Serializable;
import java.util.Random;
import java.util.UUID;

/**
 * Represents the status of a job hosted on the server.
 * @author Brad Kimmel
 */
public final class JobStatus implements Serializable {

	/** Serialization version ID. */
	private static final long serialVersionUID = 2060688367370302207L;

	/** The <code>UUID</code> idenifying the job. */
	private final UUID jobId;
	
	/** A description of the job. */
	private final String description;
	
	/** The current state of the job (new, running, complete, etc). */
	private final JobState state;
	
	/** The progress toward completion (complete = 1.0). */
	private final double progress;
	
	/** A description of the current status of the job. */
	private final String status;
	
	/** An ID uniquely identifying this status update event. */
	private final long eventId;
	
	/** The next auto-generated event ID. */
	private static long nextEventId = -Math.abs((new Random()).nextLong());
	
	/**
	 * Auto-generate an event ID.
	 * @return An auto-generated event ID.
	 */
	private static synchronized long getNextEventId() {
		return nextEventId++;
	}
	
	/**
	 * Creates a new <code>JobStatus</code>.
	 * @param jobId The <code>UUID</code> identifying the job.
	 * @param description A description of the job.
	 * @param state The current state of the job (new, running, complete, etc).
	 * @param progress The progress toward completion (complete = 1.0).  If
	 * 		equal to <code>Double.NaN</code>, the progress is indeterminant.
	 * @param status A description of the current status of the job.
	 * @param eventId An ID uniquely identifying this status update event.
	 */
	private JobStatus(UUID jobId, String description, JobState state, double progress, String status, long eventId) {
		this.jobId = jobId;
		this.description = description;
		this.state = state;
		this.progress = progress;
		this.status = status;
		this.eventId = eventId;
	}
	
	/**
	 * Creates a new <code>JobStatus</code> with no event ID.
	 * @param jobId The <code>UUID</code> identifying the job.
	 * @param description A description of the job.
	 * @param state The current state of the job (new, running, complete, etc).
	 * @param progress The progress toward completion (complete = 1.0).  If
	 * 		equal to <code>Double.NaN</code>, the progress is indeterminant.
	 * @param status A description of the current status of the job.
	 */
	public JobStatus(UUID jobId, String description, JobState state, double progress, String status) {
		this(jobId, description, state, progress, status, Long.MIN_VALUE);
	}
	
	/**
	 * Creates a new <code>JobStatus</code> with no event ID and
	 * indeterminant progress.
	 * @param jobId The <code>UUID</code> identifying the job.
	 * @param description A description of the job.
	 * @param state The current state of the job (new, running, complete, etc).
	 * @param status A description of the current status of the job.
	 */
	public JobStatus(UUID jobId, String description, JobState state, String status) {
		this(jobId, description, state, Double.NaN, status);
	}

	/**
	 * Creates a copy of this <code>JobStatus</code> with the progress set to
	 * the specified value.
	 * @param newProgress The progress toward completion (complete = 1.0).  If
	 * 		equal to <code>Double.NaN</code>, the progress is indeterminant.
	 * @return A copy of this <code>JobStatus</code> with the progress set to
	 * 		the specified value.
	 */
	public JobStatus withProgress(double newProgress) {
		return new JobStatus(jobId, description, state, newProgress, status, eventId);
	}
	
	/**
	 * Creates a copy of this <code>JobStatus</code> with the status string set
	 * to the specified value.
	 * @param newStatus A description of the current status of the job.
	 * @return A copy of this <code>JobStatus</code> with the status string set
	 * 		to the specified value.
	 */
	public JobStatus withStatus(String newStatus) {
		return new JobStatus(jobId, description, state, progress, newStatus, eventId);
	}
	
	/**
	 * Creates a copy of this <code>JobStatus</code> with the state set to
	 * {@link JobState#COMPLETE}.
	 * @return A copy of this <code>JobStatus</code> with the state set to
	 * 		{@link JobState#COMPLETE}.
	 */
	public JobStatus asComplete() {
		return new JobStatus(jobId, description, JobState.COMPLETE, 1.0, status, eventId);
	}
	
	/**
	 * Creates a copy of this <code>JobStatus</code> with the state set to
	 * {@link JobState#CANCELLED}.
	 * @return A copy of this <code>JobStatus</code> with the state set to
	 * 		{@link JobState#CANCELLED}.
	 */
	public JobStatus asCancelled() {
		return new JobStatus(jobId, description, JobState.CANCELLED, progress, status, eventId);
	}
	
	/**
	 * Creates a copy of this <code>JobStatus</code> with the progress set to
	 * an indeterminant value (<code>Double.NaN</code>).
	 * @return A copy of this <code>JobStatus</code> with the progress set to
	 * 		indeterminant.
	 */
	public JobStatus withIndeterminantProgress() {
		return new JobStatus(jobId, description, state, Double.NaN, status, eventId);
	}
	
	/**
	 * Creates a copy of this <code>JobStatus</code> with an auto-generated
	 * event ID.  The event ID will be greater than any previously generated
	 * event ID.
	 * @return A copy of this <code>JobStatus</code> with an auto-generated
	 * 		event ID.
	 */
	public JobStatus withNewEventId() {
		return new JobStatus(jobId, description, state, Double.NaN, status, getNextEventId());
	}
	
	/**
	 * Gets the <code>UUID</code> that identifies the job.
	 * @return The <code>UUID</code> that identifies the job.
	 */
	public UUID getJobId() {
		return jobId;
	}
	
	/**
	 * Gets a description of the job.
	 * @return A description of the job.
	 */
	public String getDescription() {
		return description;
	}
	
	/**
	 * Gets the current state of the job (new, running, complete, etc.).
	 * @return The current state of the job (new, running, complete, etc.).
	 */
	public JobState getState() {
		return state;
	}
	
	/**
	 * Gets a value indicating if the job is complete.
	 * @return A value indicating if the job is complete.
	 */
	public boolean isComplete() {
		return state == JobState.COMPLETE;
	}
	
	/**
	 * Gets a value indicating if the job has been cancelled.
	 * @return A value indicating if the job has been cancelled.
	 */
	public boolean isCancelled() {
		return state == JobState.CANCELLED;
	}
	
	/**
	 * Gets a value indicating the progress of the job (complete = 1.0).
	 * @return A value indicating the progress of the job.
	 */
	public double getProgress() {
		return progress;
	}
	
	/**
	 * Gets a value indicating if the progress of the job is indeterminant.
	 * @return A value indicating if the progress of the job is indeterminant.
	 */
	public boolean isProgressIndeterminant() {
		return Double.isNaN(progress);
	}
	
	/**
	 * Gets a description of the status of the job.
	 * @return A description of the status of the job.
	 */
	public String getStatus() {
		return status;
	}
	
	/**
	 * Gets the event ID.
	 * @return The event ID.
	 */
	public long getEventId() {
		return eventId;
	}

}
