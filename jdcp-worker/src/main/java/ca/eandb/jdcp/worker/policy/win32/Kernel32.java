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
import com.sun.jna.Structure;

import java.util.List;

import static com.sun.jna.win32.W32APIOptions.DEFAULT_OPTIONS;

/** Definition (incomplete) of <code>kernel32.dll</code>. */
public interface Kernel32 extends com.sun.jna.platform.win32.Kernel32 {

    Kernel32 INSTANCE = (Kernel32)
        Native.loadLibrary("kernel32", Kernel32.class, DEFAULT_OPTIONS);

  class SYSTEM_POWER_STATUS extends Structure {
    public static final List<String> FIELDS = createFieldsOrder(
        "ACLineStatus", "BatteryFlag", "BatteryLifePercent", "Reserved1", "BatteryLifeTime", "BatteryFullLifeTime");
    public byte ACLineStatus;
    public byte BatteryFlag;
    public byte BatteryLifePercent;
    public byte Reserved1;
    public int BatteryLifeTime;
    public int BatteryFullLifeTime;

    @Override
    protected List<String> getFieldOrder() {
      return FIELDS;
    }
  };

  void GetSystemPowerStatus(SYSTEM_POWER_STATUS status);
}
