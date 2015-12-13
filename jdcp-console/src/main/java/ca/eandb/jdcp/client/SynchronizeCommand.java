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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import ca.eandb.util.UnexpectedException;
import ca.eandb.util.args.AbstractCommand;
import ca.eandb.util.io.FileUtil;

/**
 * A <code>Command</code> that ensures that all in the specified directory
 * tree match those on the server.  Classes that do not exist on the server,
 * and classes which differ on the server, are uploaded to the server.
 * @author Brad Kimmel
 */
public final class SynchronizeCommand extends AbstractCommand<Configuration> {

  @Override
  protected void run(String[] args, Configuration conf) {
    for (String arg : args) {
      synchronize("", new File(arg), conf);
    }
  }

  /**
   * Synchronizes all classes in the given directory tree to the server.
   * @param pkg The name of the package associated with the root of the
   *     directory tree.
   * @param path The <code>File</code> indicating the root of the directory
   *     tree.
   * @param conf The application command line options.
   */
  private void synchronize(String pkg, File path, Configuration conf) {
    if (!path.isDirectory()) {
      throw new IllegalArgumentException(path.getAbsolutePath().concat(" is not a directory."));
    }

    for (File file : path.listFiles()) {
      if (file.isDirectory()) {
        synchronize(combine(pkg, file.getName()), file, conf);
      } else {
        String fileName = file.getName();
        int extensionSeparator = fileName.lastIndexOf('.');
        if (extensionSeparator >= 0) {
          String extension = fileName.substring(extensionSeparator + 1);
          if (extension.equals("class")) {
            String className = combine(pkg, fileName.substring(0, extensionSeparator));
            try {
              byte[] digest = conf.getJobService().getClassDigest(className);
              byte[] def = FileUtil.getFileContents(file);
              byte[] localDigest = getDigest(def, conf);
              if (digest == null || !Arrays.equals(digest, localDigest)) {
                conf.getJobService().setClassDefinition(className, def);
                System.out.print(digest == null ? "+ " : "U ");
                System.out.println(className);
              } else if (conf.verbose) {
                System.out.print("= ");
                System.out.println(className);
              }
            } catch (FileNotFoundException e) {
              throw new UnexpectedException(e);
            } catch (IOException e) {
              System.out.print("E ");
              System.out.println(className);
            }
          }
        }
      }
    }
  }

  /**
   * Gets the digest of the specified class definition.
   * @param def The class definition.
   * @param conf The application command line options.
   * @return The digest of the class definition.
   */
  private byte[] getDigest(byte[] def, Configuration conf) {
    try {
      MessageDigest alg = MessageDigest.getInstance(conf.digestAlgorithm);
      return alg.digest(def);
    } catch (NoSuchAlgorithmException e) {
      throw new UnexpectedException(e);
    }
  }

  /**
   * Combines package path.
   * @param parent The parent package.
   * @param child The name of the child package.
   * @return The combined package name.
   */
  private String combine(String parent, String child) {
    if (parent.length() > 0) {
      return parent.concat(".").concat(child);
    } else {
      return child;
    }
  }

}
