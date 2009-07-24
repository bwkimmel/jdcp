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

package ca.eandb.jdcp.worker.mpi;

import java.util.concurrent.ThreadFactory;

import org.apache.log4j.PropertyConfigurator;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.jdcp.worker.JobServiceFactory;
import ca.eandb.jdcp.worker.ThreadServiceWorker;
import ca.eandb.util.concurrent.BackgroundThreadFactory;
import ca.eandb.util.progress.ConsoleProgressMonitor;
import ca.eandb.util.progress.DummyProgressMonitorFactory;
import ca.eandb.util.progress.ProgressMonitor;
import ca.eandb.util.progress.ProgressMonitorFactory;

/**
 * @author brad
 *
 */
public final class MpiWorkerMain {

	/**
	 * @param args
	 */
	public static void start() {
		JdcpUtil.initialize();
		PropertyConfigurator.configure(System.getProperties());

		JobServiceFactory serviceFactory = new JobServiceFactory() {
			public JobService connect() {
				return new MpiWorkerJobService();
			}
		};
		ThreadFactory threadFactory = new BackgroundThreadFactory();
		ProgressMonitorFactory monitorFactory = DummyProgressMonitorFactory.getInstance();
//		new ProgressMonitorFactory() {
//			public ProgressMonitor createProgressMonitor(String title) {
//				return new ConsoleProgressMonitor(40);
//			}
//		};
		ThreadServiceWorker worker = new ThreadServiceWorker(serviceFactory, threadFactory, monitorFactory);
		worker.setMaxWorkers(1);
		worker.run();
	}

}
