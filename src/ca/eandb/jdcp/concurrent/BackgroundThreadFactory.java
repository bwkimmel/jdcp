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

package ca.eandb.jdcp.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * A <code>ThreadFactory</code> that creates daemon threads of low priority.
 * @author Brad Kimmel
 */
public final class BackgroundThreadFactory implements ThreadFactory {

	/**
	 * Creates a new <code>BackgroundThreadFactory</code>.
	 */
	public BackgroundThreadFactory() {
		this.inner = Executors.defaultThreadFactory();
	}

	/**
	 * Creates a new <code>BackgroundThreadFactory</code>.
	 * @param inner The <code>ThreadFactory</code> to use to create new
	 * 		threads (must not be null).
	 * @throws IllegalArgumentException if <code>inner</code> is null.
	 */
	public BackgroundThreadFactory(ThreadFactory inner) throws IllegalArgumentException {
		if (this.inner == null) {
			throw new IllegalArgumentException("inner must not be null.");
		}
		this.inner = inner;
	}

	/* (non-Javadoc)
	 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
	 */
	public Thread newThread(Runnable r) {

		/* Use the inner ThreadFactory to create a new thread. */
		Thread thread = this.inner.newThread(r);

		/* Make the thread a daemon thread that runs at the lowest possible
		 * priority.
		 */
		thread.setDaemon(true);
		thread.setPriority(Thread.MIN_PRIORITY);

		return thread;

	}

	/** The thread factory to use to create new threads. */
	private final ThreadFactory inner;

}
