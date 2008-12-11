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

package ca.eandb.jdcp.worker;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.PrintStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import javax.security.auth.login.LoginException;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;
import javax.swing.text.Document;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import ca.eandb.jdcp.remote.AuthenticationService;
import ca.eandb.jdcp.remote.JobService;
import ca.eandb.util.io.DocumentOutputStream;
import ca.eandb.util.progress.ProgressPanel;

/**
 * @author Brad Kimmel
 *
 */
public final class MainWindow extends JFrame {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(MainWindow.class);  //  @jve:decl-index=0:
	private JPanel jContentPane = null;
	private JSplitPane jSplitPane = null;
	private ProgressPanel progressPanel = null;
	private JEditorPane consolePane = null;
	private JScrollPane consoleScrollPane = null;
	private ConnectionDialog connectionDialog = null;

	/**
	 * This method initializes jSplitPane
	 *
	 * @return javax.swing.JSplitPane
	 */
	private JSplitPane getJSplitPane() {
		if (jSplitPane == null) {
			jSplitPane = new JSplitPane();
			jSplitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
			jSplitPane.setDividerLocation(100);
			jSplitPane.setOneTouchExpandable(true);
			jSplitPane.setTopComponent(getProgressPanel());
			jSplitPane.setBottomComponent(getConsoleScrollPane());
			jSplitPane.setResizeWeight(1.0D);
		}
		return jSplitPane;
	}

	/**
	 * This method initializes progressPanel
	 *
	 * @return ca.eandb.util.progress.ProgressPanel
	 */
	private ProgressPanel getProgressPanel() {
		if (progressPanel == null) {
			progressPanel = new ProgressPanel();
		}
		return progressPanel;
	}

	/**
	 * This method initializes consolePane
	 *
	 * @return javax.swing.JEditorPane
	 */
	private JEditorPane getConsolePane() {
		if (consolePane == null) {
			consolePane = new JEditorPane();
			consolePane.setEditable(false);
			consolePane.setContentType("text/rtf");
		}
		return consolePane;
	}

	/**
	 * This method initializes consoleScrollPane
	 *
	 * @return javax.swing.JScrollPane
	 */
	private JScrollPane getConsoleScrollPane() {
		if (consoleScrollPane == null) {
			consoleScrollPane = new JScrollPane();
			consoleScrollPane.setViewportView(getConsolePane());
		}
		return consoleScrollPane;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ConsoleAppender appender = new ConsoleAppender();
		appender.setLayout(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN));
		appender.setTarget(ConsoleAppender.SYSTEM_ERR);
		appender.setFollow(true);
		appender.activateOptions();
		BasicConfigurator.configure(appender);

		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				MainWindow thisClass = new MainWindow();
				thisClass.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
				thisClass.setVisible(true);
			}
		});
	}

	/**
	 * This is the default constructor
	 */
	public MainWindow() {
		super();
		initialize();
	}

	/**
	 * This method initializes this
	 *
	 * @return void
	 */
	private void initialize() {
		this.setSize(300, 200);
		this.setContentPane(getJContentPane());
		this.setTitle("JDCP Worker");
		this.addWindowListener(new WindowAdapter() {
			public void windowOpened(WindowEvent e) {
				connectConsole();
				startWorker();
			}
		});
	}

	private JobService connect() {
		JobService service = null;
		do {
			ConnectionDialog dialog = getConnectionDialog();
			dialog.setVisible(true);
			if (dialog.isCancelled()) {
				break;
			}
			service = connect(dialog.getHost(), dialog.getUser(), dialog.getPassword());
		} while (service == null);
		return service;
	}

	private JobService connect(String host, String user, String password) {
		try {
			Registry registry = LocateRegistry.getRegistry(host);
			AuthenticationService authService = (AuthenticationService) registry.lookup("AuthenticationService");
			return authService.authenticate(user, password);
		} catch (LoginException e) {
			logger.error("Authentication failed.", e);
			JOptionPane.showMessageDialog(this, "Authentication failed.  Please check your user name and password.", "Connection Failed", JOptionPane.WARNING_MESSAGE);
		} catch (RemoteException e) {
			logger.error("Could not connect to remote host.", e);
			JOptionPane.showMessageDialog(this, "Could not connect to remote host.", "Connection Failed", JOptionPane.WARNING_MESSAGE);
		} catch (NotBoundException e) {
			logger.error("Could not find AuthenticationService at remote host.", e);
			JOptionPane.showMessageDialog(this, "Could find JDCP Server at remote host.", "Connection Failed", JOptionPane.WARNING_MESSAGE);
		}
		return null;
	}

	/**
	 * Start the worker thread.
	 */
	private void startWorker() {
		JobService service = connect();
		if (service != null) {
			// TODO start worker thread
		} else {
			setVisible(false);
		}
	}

	/**
	 * This method initializes jContentPane
	 *
	 * @return javax.swing.JPanel
	 */
	private JPanel getJContentPane() {
		if (jContentPane == null) {
			jContentPane = new JPanel();
			jContentPane.setLayout(new BorderLayout());
			jContentPane.add(getJSplitPane(), BorderLayout.CENTER);
		}
		return jContentPane;
	}

	private ConnectionDialog getConnectionDialog() {
		if (connectionDialog == null) {
			connectionDialog = new ConnectionDialog(this);
		}
		return connectionDialog;
	}

	public void connectConsole() {
		Document document = getConsolePane().getDocument();

		System.setOut(new PrintStream(new DocumentOutputStream(document, SimpleAttributeSet.EMPTY)));

		MutableAttributeSet attributes = new SimpleAttributeSet();
		StyleConstants.setForeground(attributes, Color.RED);
		System.setErr(new PrintStream(new DocumentOutputStream(document, attributes)));
	}

}
