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

import java.rmi.RemoteException;

import ca.eandb.util.args.AbstractCommand;

/**
 * @author Brad Kimmel
 *
 */
public class SetIdleTimeCommand extends AbstractCommand<Configuration> {

	/* (non-Javadoc)
	 * @see ca.eandb.jdcp.client.Command#run(java.util.List, ca.eandb.jdcp.client.Configuration)
	 */
	public void run(String[] args, Configuration conf) {
		int seconds = Integer.parseInt(args[0]);
		try {
			conf.getJobService().setIdleTime(seconds);
		} catch (IllegalArgumentException e) {
			System.err.println("Invalid priority: " + args[0]);
		} catch (SecurityException e) {
			System.err.println("Access denied.");
		} catch (RemoteException e) {
			System.err.println("Failed to set idle time on remote host.");
			e.printStackTrace();
		}
	}

}
