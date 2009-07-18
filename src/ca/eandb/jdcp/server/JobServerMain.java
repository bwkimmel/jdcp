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

package ca.eandb.jdcp.server;

import java.awt.Dimension;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.prefs.Preferences;

import javax.swing.JFrame;

import org.apache.derby.jdbc.EmbeddedDataSource;

import ca.eandb.jdcp.JdcpUtil;
import ca.eandb.jdcp.server.classmanager.DbClassManager;
import ca.eandb.jdcp.server.scheduling.PrioritySerialTaskScheduler;
import ca.eandb.jdcp.server.scheduling.TaskScheduler;
import ca.eandb.util.progress.ProgressPanel;

/**
 * The main class for the JDCP server application.
 * @author Brad Kimmel
 */
public final class JobServerMain {

	/**
	 * Runs the JDCP server application.
	 * @param args Command line arguments.
	 */
	public static void main(String[] args) {
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				startServer();
			}
		});
	}

	/**
	 * Starts the JDCP server.
	 */
	private static void startServer() {

		try {

			JdcpUtil.initialize();

			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			EmbeddedDataSource ds = new EmbeddedDataSource();
			ds.setConnectionAttributes("create=true");
			ds.setDatabaseName("classes");

			System.err.print("Initializing progress monitor...");
			ProgressPanel panel = new ProgressPanel();
			panel.setPreferredSize(new Dimension(500, 350));
			System.err.println("OK");

			System.err.print("Initializing folders...");
			Preferences pref = Preferences
					.userNodeForPackage(JobServer.class);
			String path = pref.get("rootDirectory", "./server");
			File rootDirectory = new File(path);
			File jobsDirectory = new File(rootDirectory, "jobs");

			rootDirectory.mkdir();
			jobsDirectory.mkdir();
			System.err.println("OK");

			System.err.print("Initializing service...");
			DbClassManager classManager = new DbClassManager(ds);
			classManager.prepareDataSource();

			TaskScheduler scheduler = new PrioritySerialTaskScheduler();
			Executor executor = Executors.newCachedThreadPool();
			JobServer jobServer = new JobServer(jobsDirectory, panel, scheduler, classManager, executor);
			AuthenticationServer authServer = new AuthenticationServer(jobServer, 5327);
			System.err.println("OK");

			System.err.print("Exporting service stubs...");
//			JobService jobStub = (JobService) UnicastRemoteObject.exportObject(
//					jobServer, 0);
			System.err.println("OK");

			System.err.print("Binding service...");
			final Registry registry = LocateRegistry.createRegistry(5327);
			registry.bind("AuthenticationService", authServer);
			System.err.println("OK");

			System.err.println("Server ready");

			JFrame frame = new JFrame("JDCP Server");
			frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

			frame.getContentPane().add(panel);
			frame.pack();

			frame.addWindowListener(new WindowAdapter() {
				public void windowClosed(WindowEvent event) {
					System.err.print("Shutting down...");
					try {
						registry.unbind("AuthenticationService");
						System.err.println("OK");
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.exit(0);
				}
			});

			frame.setVisible(true);

		} catch (Exception e) {

			System.err.println("Server exception:");
			e.printStackTrace();

		}

	}

}
