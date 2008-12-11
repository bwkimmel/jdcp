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

import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.SwingUtilities;
import javax.swing.text.Document;
import javax.swing.text.MutableAttributeSet;
import javax.swing.text.SimpleAttributeSet;
import javax.swing.text.StyleConstants;

import ca.eandb.util.io.DocumentOutputStream;
import ca.eandb.util.progress.ProgressPanel;

/**
 * @author Brad Kimmel
 *
 */
public final class MainWindow extends JFrame {

	private static final long serialVersionUID = 1L;
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

	/**
	 * Start the worker thread.
	 */
	private void startWorker() {
		ConnectionDialog dialog = getConnectionDialog();
		dialog.setVisible(true);

		if (dialog.isCancelled()) {
			setVisible(false);
		} else {
			// TODO set up connection
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
