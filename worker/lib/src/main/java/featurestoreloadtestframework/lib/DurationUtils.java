package featurestoreloadtestframework.lib;

import java.time.Duration;

public class DurationUtils {
  /**
   * Convert a duration to a floating point millisecond number.
   * @param duration The duration to convert.
   * @return The number of milliseconds with decimal places.
   */
  public static double toMillis(Duration duration) {
    double numberOfNanosecondsInAMillisecond = 1e6D;
    return duration.toNanos() / numberOfNanosecondsInAMillisecond;
  }

}
