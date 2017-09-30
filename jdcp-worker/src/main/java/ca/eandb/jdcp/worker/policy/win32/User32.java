/* Copyright (c) 2007 Timothy Wall, All Rights Reserved
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * <p/>
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 */
package ca.eandb.jdcp.worker.policy.win32;

import com.sun.jna.Native;

import static com.sun.jna.win32.W32APIOptions.DEFAULT_OPTIONS;

/** Provides access to the w32 user32 library.
 * Incomplete implementation to support demos.
 *
 * @author  Todd Fast, todd.fast@sun.com
 * @author twall@users.sf.net
 */
public interface User32 extends com.sun.jna.platform.win32.User32 {
  User32 INSTANCE = (User32) Native.loadLibrary("user32", User32.class, DEFAULT_OPTIONS);

  LRESULT CallWindowProc(WindowProc lpPrevWndFunc, HWND hWnd, int uMsg, WPARAM wParam, LPARAM lParam);
  LRESULT CallWindowProc(int lpPrevWndFunc, HWND hWnd, int uMsg, WPARAM wParam, LPARAM lParam);
  int SetWindowLong(HWND hWnd, int nIndex, WindowProc dwNewLong);

  int WM_POWERBROADCAST = 536;

    // WPARAM values for WM_POWERBROADCAST message
  int PBT_APMPOWERSTATUSCHANGE = 0xA;
  int PBT_APMRESUMEAUTOMATIC = 0x12;
  int PBT_APMRESUMESUSPEND = 0x7;
  int PBT_APMSUSPEND = 0x4;
  int PBT_POWERSETTINGCHANGE = 0x8013;

  // WPARAM value for WM_POWERBROADCAST message (WS2003, XP, W2000)
  int PBT_APMBATTERYLOW = 0x9;
  int PBT_APMOEMEVENT = 0xB;
  int PBT_APMQUERYSUSPEND = 0x0;
  int PBT_APMQUERYSUSPENDFAILED = 0x2;
  int PBT_APMRESUMECRITICAL = 0x6;

  /**
   * May be returned from WM_POWERBROADCAST in response to message with
   * WPARAM value PBT_APMQUERYSUSPEND or PBT_APMQUERYSUSPENDFAILED
   * (WS2003, XP, W2000).
   */
  int BROADCAST_QUERY_DENY = 0x424D5144;

}
