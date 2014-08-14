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

package ca.eandb.jdcp.remote;

/**
 * A <code>RuntimeException</code> caused by failure of a hub to delegate a
 * <code>JobService</code> request to a connected server.
 * @author Brad Kimmel
 */
public final class DelegationException extends RuntimeException {

  /**
   * Serialization version ID.
   */
  private static final long serialVersionUID = -8052416531056121484L;

  /**
   * Creates a new <code>DelegationException</code>.
   */
  public DelegationException() {
    super();
  }

  /**
   * Creates a new <code>DelegationException</code>.
   * @param message A description of the exceptional condition.
   * @param cause The cause of the exceptional condition.
   */
  public DelegationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a new <code>DelegationException</code>.
   * @param message A description of the exceptional condition.
   */
  public DelegationException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>DelegationException</code>.
   * @param cause The cause of the exceptional condition.
   */
  public DelegationException(Throwable cause) {
    super(cause);
  }

}
