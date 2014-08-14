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
 * An object that decides whether tasks should be running (and hence using the
 * CPU) right now.  Whether or not tasks should be running is up to the
 * concrete class, and may depend on factors such as whether this machine is
 * on battery power, whether there has been keyboard or mouse activity
 * recently, etc.
 * @author Brad Kimmel
 */
public interface CourtesyMonitor {

	/**
	 * Determines whether tasks should be allowed to run.
	 * @return A value indicating whether tasks should be allowed to run.
	 */
	boolean allowTasksToRun();

	/**
	 * Waits until tasks are allowed to run.
	 * @throws InterruptedException If this thread was interrupted.
	 */
	void waitFor() throws InterruptedException;

}
