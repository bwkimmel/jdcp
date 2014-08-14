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

package ca.eandb.jdcp.server.classmanager;

import java.nio.ByteBuffer;

import ca.eandb.util.classloader.ClassLoaderStrategy;

/**
 * Manages storage and retrieval of class definitions.  May be used as a
 * <code>ClassLoaderStrategy</code> in a <code>StrategyClassLoader</code>.
 * @see ca.eandb.util.classloader.ClassLoaderStrategy
 * @see ca.eandb.util.classloader.StrategyClassLoader
 * @author Brad Kimmel
 */
public interface ClassManager extends ClassLoaderStrategy {

  /**
   * Sets a class definition.
   * @param name The fully qualified name of the class to define.
   * @param def A <code>ByteBuffer</code> containing the definition of the
   *     class.
   */
  void setClassDefinition(String name, ByteBuffer def);

  /**
   * Sets a class definition.
   * @param name The fully qualified name of the class to define.
   * @param def The definition of the class.
   */
  void setClassDefinition(String name, byte[] def);

  /**
   * Gets the MD5 digest of the class definition.
   * @param name The fully qualified name of the class.
   * @return The MD5 digest of the class definition.
   */
  byte[] getClassDigest(String name);

}
