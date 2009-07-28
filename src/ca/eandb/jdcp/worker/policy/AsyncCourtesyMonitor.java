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

package ca.eandb.jdcp.worker.policy;

/**
 * A <code>CourtesyMonitor</code> whose state is updated asynchronously. For
 * example, by polling or by the handling of external events.
 * @author Brad Kimmel
 */
public abstract class AsyncCourtesyMonitor implements CourtesyMonitor {

	/** A flag indicating if tasks should be allowed to run. */
	private boolean allow;

	/**
	 * Sets whether tasks should be allowed to run.
	 * @param state A value indicating whether tasks should be allowed to run.
	 */
	protected final void allow(boolean state) {
		if (state) {
			allow();
		} else {
			disallow();
		}
	}

	/**
	 * Allows tasks to run.
	 * Equivalent to <code>allow(true)</code>.
	 * @see #allow(boolean)
	 */
	protected synchronized final void allow() {
		if (!allow) {
			allow = true;
			notifyAll();
		}
	}

	/**
	 * Prevents tasks from running.
	 * Equivalent to <code>allow(false)</code>.
	 * @see #allow(boolean)
	 */
	protected synchronized final void disallow() {
		allow = false;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.CourtesyMonitor#allowTasksToRun()
	 */
	public final synchronized boolean allowTasksToRun() {
		return allow;
	}

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.worker.policy.CourtesyMonitor#waitFor()
	 */
	public final synchronized void waitFor() throws InterruptedException {
		if (!allow) {
			wait();
		}
	}

}
