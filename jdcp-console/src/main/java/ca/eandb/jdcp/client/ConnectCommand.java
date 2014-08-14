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

import java.util.Queue;

import ca.eandb.util.args.ArgumentProcessor;
import ca.eandb.util.args.Command;
import ca.eandb.util.args.StringFieldOption;

/**
 * @author Brad
 *
 */
public final class ConnectCommand implements Command<Configuration> {

	private static final ArgumentProcessor<Configuration> argProcessor = new ArgumentProcessor<Configuration>();

	static {
		argProcessor.addOption("host", 'h', new StringFieldOption<Configuration>("host"));
		argProcessor.addOption("username", 'u', new StringFieldOption<Configuration>("username"));
		argProcessor.addOption("password", 'p', new StringFieldOption<Configuration>("password"));
		argProcessor.setDefaultCommand(new Command<Configuration>() {
			public void process(Queue<String> argq, Configuration state) {
				state.getJobService();
			}
		});
	}

	/* (non-Javadoc)
	 * @see ca.eandb.util.args.AbstractCommand#process(java.util.Queue, java.lang.Object)
	 */
	public void process(Queue<String> argq, Configuration state) {
		argProcessor.process(argq, state);
	}

}
