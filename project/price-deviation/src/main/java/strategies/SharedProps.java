// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.io.FileWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicBoolean;


public class SharedProps {
  public static StrategyConfigs configs = new StrategyConfigs();

  public static AccountManagement acc_man = null;

  public static String strategy_dir;
  //
  public static String newLine="<br>";

  public static long get_sys_ts() {
    return System.currentTimeMillis() + configs.server_time_offset;
  }
  private static long timeMillisSet = 0;
  public static long get_ts() {
    return timeMillisSet;
  }
  public static void set_ts(long time) {
    if(time < timeMillisSet) return;
    timeMillisSet = time;
  }

  public static ConcurrentHashMap<String, Boolean>        inst_active = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<String, Long>           oBusy = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<String, AtomicBoolean>  inst_gain_change_state = new ConcurrentHashMap<>();
  public static ConcurrentLinkedQueue<String>             reports_q = new ConcurrentLinkedQueue<>();


  public static double round(double amount, int decimalPlaces) {
    return (new BigDecimal(amount)).setScale(decimalPlaces, RoundingMode.HALF_UP).doubleValue();
  }


  private static ReentrantLock  log_lock = new ReentrantLock();
  private static int            log_day = 0;
  private static FileWriter     log_fd = null;

  public static void print(Object o) {
    log_lock.lock();
    try {
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      if(log_day != cal.get(Calendar.DATE)) {
        if(log_fd != null)
          log_fd.close();
        log_day = cal.get(Calendar.DATE);
        log_fd = new FileWriter(strategy_dir + "logs/" + cal.get(Calendar.YEAR) + "-" + (cal.get(Calendar.MONTH) + 1) + "-" + log_day + ".log");
      }
      log_fd.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS: ").format(new Date()));
      log_fd.append(o + "\n");
      log_fd.flush();
    } catch (Exception e) { }
    log_lock.unlock();
  }


  private static double free_margins = 0.0;
  public static synchronized double get_free_margin() { return free_margins; }
  public static synchronized void set_free_margin(double v) { free_margins = v; }

  private static double leverage_used = 0.0;
  public static synchronized double get_leverage_used() { return leverage_used; }
  public static synchronized void set_leverage_used(double v) { leverage_used = v; }


}
