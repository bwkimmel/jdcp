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

/**
 * A <code>ClassManager</code> that was created by a
 * <code>ParentClassManager</code>.
 * @see ParentClassManager#createChildClassManager()
 * @author Brad Kimmel
 */
public interface ChildClassManager extends ClassManager {

  /**
   * Gets the identifier for this <code>ChildClassManager</code>.  This
   * identifier may be used to obtain an instance of this
   * <code>ChildClassManager</code> using
   * {@link ParentClassManager#getChildClassManager(int)}.
   * @return The identifier for this <code>ChildClassManager</code>.
   * @see ParentClassManager#getChildClassManager(int)
   */
  int getChildId();

  /**
   * Releases this <code>ChildClassManager</code>.  Any further use of this
   * <code>ClassManager</code> after this method is called may result in an
   * <code>IllegalStateException</code> being thrown.
   */
  void release();

}
