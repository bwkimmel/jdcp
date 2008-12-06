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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Queue;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import ca.eandb.util.args.AbstractCommand;
import ca.eandb.util.args.ArgumentProcessor;
import ca.eandb.util.args.Command;
import ca.eandb.util.args.FileFieldOption;
import ca.eandb.util.args.StringFieldOption;

/**
 * @author brad
 *
 */
public final class ScriptCommand implements Command<Configuration> {

	private static final String DEFAULT_LANGUAGE = "JavaScript";

	public static class Options {

		public File file;

		public String language = null;

	}

	public void process(Queue<String> argq, final Configuration conf) {

		ArgumentProcessor<Options> argProcessor = new ArgumentProcessor<Options>();

		argProcessor.addOption("file", 'f', new FileFieldOption<Options>("file", true));
		argProcessor.addOption("language", 'l', new StringFieldOption<Options>("language"));

		argProcessor.setDefaultCommand(new AbstractCommand<Options>() {

			@Override
			protected void run(String[] args, Options options) {

				try {
					ScriptEngineManager factory = new ScriptEngineManager();
					ScriptEngine engine = getScriptEngine(factory, options);
					if (engine == null) {
						System.err.println("Unrecognized language");
						System.exit(1);
					}
					InputStream in = (options.file != null)
							? new FileInputStream(options.file)
							: System.in;
					Reader reader = new InputStreamReader(in);

					engine.put("jdcp", new ScriptFacade(conf));
					engine.put("args", args);
					engine.eval(reader);
				} catch (ScriptException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		});

		argProcessor.process(argq, new Options());

	}

	private ScriptEngine getScriptEngine(ScriptEngineManager factory, Options options) {
		if (options.language != null) {
			return factory.getEngineByName(options.language);
		}
		if (options.file != null) {
			String fileName = options.file.getName();
			int separator = fileName.lastIndexOf('.');
			if (separator < 0) {
				String extension = fileName.substring(separator + 1);
				return factory.getEngineByExtension(extension);
			}
		}
		return factory.getEngineByName(DEFAULT_LANGUAGE);
	}

}