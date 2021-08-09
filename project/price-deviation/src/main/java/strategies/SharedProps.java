// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SharedProps {
  public static StrategyConfigs configs = new StrategyConfigs();

  public static AccountManagement acc_man = null;

  public static String strategy_dir;
  //
  public static String newLine="<br>";

  public static long get_sys_ts() {
    return System.currentTimeMillis();
  }
  private static long timeMillisSet = 0;
  public static long get_ts() {
    return timeMillisSet;
  }
  public static void set_ts(long time) {
    if(time < timeMillisSet) return;
    timeMillisSet = time;
  }

  public static boolean amount_balanced_by_margin_reached = false;


  public static ConcurrentLinkedQueue<String> log_q = new ConcurrentLinkedQueue<>();
  public static ConcurrentLinkedQueue<String> reports_q = new ConcurrentLinkedQueue<>();

  public static ConcurrentHashMap<String, Boolean> inst_active= new ConcurrentHashMap<>();
  public static ConcurrentHashMap<String, Double> inst_amt_ratio = new ConcurrentHashMap<>();
  public static ConcurrentHashMap<String, Double> inst_std_dev_avg= new ConcurrentHashMap<>();
  public static ConcurrentHashMap<String, Long> oBusy= new ConcurrentHashMap<>();
  public static ConcurrentHashMap<String, Long> curr_a_ts= new ConcurrentHashMap<>();

  public static double round(double amount, int decimalPlaces) {
    return (new BigDecimal(amount)).setScale(decimalPlaces, RoundingMode.HALF_UP).doubleValue();
  }

  public static void print (Object o){
    log_q.add(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())+":"+o);
  }


}
