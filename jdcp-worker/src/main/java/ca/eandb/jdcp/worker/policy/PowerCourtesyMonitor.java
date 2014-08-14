package ca.eandb.jdcp.worker.policy;

public interface PowerCourtesyMonitor extends CourtesyMonitor {

  /**
   * @return the requireAC
   */
  boolean isRequireAC();

  /**
   * @param requireAC the requireAC to set
   */
  void setRequireAC(boolean requireAC);

  /**
   * @return the minBatteryLifePercent
   */
  int getMinBatteryLifePercent();

  /**
   * @param minBatteryLifePercent the minBatteryLifePercent to set
   */
  void setMinBatteryLifePercent(int minBatteryLifePercent);

  /**
   * @return the minBatteryLifePercentWhileCharging
   */
  int getMinBatteryLifePercentWhileCharging();

  /**
   * @param minBatteryLifePercentWhileCharging the minBatteryLifePercentWhileCharging to set
   */
  void setMinBatteryLifePercentWhileCharging(
      int minBatteryLifePercentWhileCharging);

  void update();

}
