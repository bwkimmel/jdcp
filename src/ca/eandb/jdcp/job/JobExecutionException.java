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

package ca.eandb.jdcp.job;

/**
 * An <code>Exception</code> wrapper for exceptions thrown by a
 * <code>ParallelizableJob</code>.
 * @author Brad Kimmel
 * @see ca.eandb.jdcp.job.ParallelizableJob
 */
public final class JobExecutionException extends Exception {

	/**
	 * Serialization Version ID.
	 */
	private static final long serialVersionUID = -8692323071682734864L;

	/**
	 * Creates a new <code>JobExecutionException</code>.
	 * @param cause The cause of this exception.
	 */
	public JobExecutionException(Throwable cause) {
		super(cause);
	}

	/**
	 * Creates a new <code>JobExecutionException</code>.
	 * @param message A message describing the exceptional condition.
	 * @param cause The cause of this exception.
	 */
	public JobExecutionException(String message, Throwable cause) {
		super(message, cause);
	}

}
