// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.Instrument;
import com.dukascopy.api.ICurrency;
import com.dukascopy.api.Period;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;



public class StrategyConfigs {

  public ICurrency account_currency;
  public long      server_time_offset = 0;

  static public Instrument[] instruments;

  static public Instrument[] getStrategyInstruments() {
    Instrument[] insts = {};

    /*
    insts = Instrument.values();
    SharedProps.print("Possible instruments: (" + insts.length + ")");
    for(Instrument inst : insts) {
      SharedProps.print(" > " + inst );
      SharedProps.inst_active.put(inst.toString(), false);
    }
    SharedProps.print(" ---------------------------- ");
    if(insts.length > 0)
      return insts;
    insts = new Instrument[]{};
    */

    SharedProps.print("Reading Config |" + SharedProps.strategy_dir + "instruments.conf|");
    File file = new File(SharedProps.strategy_dir + "instruments.conf");
    if(!file.exists())
      return insts;
    try {
      FileReader fr = new FileReader(file);
      BufferedReader reader  = new BufferedReader(fr);
      String line;
      while ((line = reader.readLine()) != null) {
        Instrument inst = Instrument.fromString(line);
        if(inst == null || !Instrument.contains(line)) {
          SharedProps.print("config skipped: |"+line+"|");
          continue;
        }
        insts = Arrays.copyOf(insts, insts.length + 1);
        insts[insts.length - 1] = inst;
        SharedProps.inst_active.put(inst.toString(), false);
      }
      fr.close();

      SharedProps.print("Using instruments: (" + insts.length + ")");
      for(Instrument inst : insts) {
        SharedProps.print(" > " + inst );
      }
      SharedProps.print(" ---------------------------- ");
    } catch (Exception e) {
      SharedProps.print(e);
    }
    return insts;
  }

  public AtomicBoolean debug               = new AtomicBoolean(false);
  public AtomicBoolean trail_managed       = new AtomicBoolean(false);
  public AtomicBoolean trail_at_one_side   = new AtomicBoolean(false);
  public AtomicBoolean with_positive_flat  = new AtomicBoolean(true);


  public AtomicInteger slippage                 = new AtomicInteger(0);
  public AtomicInteger equity_gain_change_count = new AtomicInteger(3);


  public AtomicLong timezone_offset                   = new AtomicLong(0);
  public AtomicLong execute_inst_ms                   = new AtomicLong(507);
  public AtomicLong execute_inst_delay_ms             = new AtomicLong(10000);
  public AtomicLong execute_inst_delay_init_ms        = new AtomicLong(10000);
  public AtomicLong last_execution_ts                 = new AtomicLong(0);
  public AtomicLong execute_inst_skip_weekend_secs    = new AtomicLong(21600);
  public AtomicLong execute_inst_skip_day_end_secs    = new AtomicLong(10800);
  public AtomicLong execute_inst_skip_day_begin_secs  = new AtomicLong(7200);
  public AtomicLong ticks_history_secs                = new AtomicLong(3600);
  public AtomicLong ticks_history_ttl                 = new AtomicLong(1800);

  public AtomicLong cfg_file_change       = new AtomicLong(0);
  public AtomicLong cfg_ts                = new AtomicLong(0);


  public class SyncDouble {
    double value;
    public SyncDouble(double v) { value = v; }
    public synchronized void set(double v) { value = v; }
    public synchronized double get() { return value; }
  };

  public SyncDouble equity_gain_base                        = new SyncDouble(0);
  public SyncDouble equity_gain_change                      = new SyncDouble(0.10);
  public SyncDouble amount                                  = new SyncDouble(1.0);
  public SyncDouble merge_close_neg_side_multiplier         = new SyncDouble(7.00);
  public SyncDouble merge_close_neg_side_growth_rate        = new SyncDouble(0.001);
  public SyncDouble equity_gain_state_close_step_multiplier = new SyncDouble(4.00);
  public SyncDouble open_followup_amount_muliplier          = new SyncDouble(1.0);
  public SyncDouble open_followup_step_offers_diff_devider  = new SyncDouble(10.0);
  public SyncDouble open_followup_step_first_muliplier      = new SyncDouble(8.00);
  public SyncDouble open_followup_step_first_upto_ratio     = new SyncDouble(10.00);
  public SyncDouble open_followup_step_muliplier            = new SyncDouble(25.0);
  public SyncDouble open_followup_require_growth_rate       = new SyncDouble(0.10);
  public SyncDouble open_followup_flat_amt_muliplier        = new SyncDouble(0.00);
  public SyncDouble flat_positive_amt_ratio                 = new SyncDouble(1.00);
  public SyncDouble flat_positive_profit_part               = new SyncDouble(0.80);
  public SyncDouble order_zero_base_step_multiplier         = new SyncDouble(0.00);
  public SyncDouble trail_step_1st_min                      = new SyncDouble(1.00);
  public SyncDouble trail_step_1st_divider                  = new SyncDouble(1.00);
  public SyncDouble trail_step_entry_multiplier             = new SyncDouble(5.0);
  public SyncDouble trail_step_rest_plus_gain               = new SyncDouble(0.20);
  public SyncDouble email_equity_gain_change                = new SyncDouble(0.05);


  public class SyncString {
    String value;
    public SyncString(String v) { value = v; }
    public synchronized void set(String v) { value = v; }
    public synchronized String get() { return value; }
    public synchronized boolean isEmpty() { return value.isEmpty(); }
  };

  public SyncString reportEmail = new SyncString("");
  public SyncString reportName  = new SyncString("");



  public void getConfigs() {
    try {
      if(cfg_ts.get() > System.currentTimeMillis())
        return;
      cfg_ts.set(System.currentTimeMillis() + 60 * 1000);

      File file = new File(SharedProps.strategy_dir + "default.conf");
      if(!file.exists())
        return;

      long mod = file.lastModified();
      if(cfg_file_change.get() == mod)
        return;
      cfg_file_change.set(mod);

      FileReader fr = new FileReader(file);
      BufferedReader reader  = new BufferedReader(fr);

      String line;
      while ((line = reader.readLine()) != null) {
        if(line.length() == 0 || line.startsWith("#"))
          continue;
        SharedProps.print("setting='" + line + "'");

        String[] config = line.split(":");
        switch(config[0]) {

          // bool
          case "debug":
            debug.set(config[1].equals("true"));
            SharedProps.print(config[0] + " set to: " + debug.get() );
            break;
          case "trail_managed":
            trail_managed.set(config[1].equals("true"));
            SharedProps.print(config[0] + " set to: " + trail_managed.get() );
            break;
          case "trail_at_one_side":
            trail_at_one_side.set(config[1].equals("true"));
            SharedProps.print(config[0] + " set to: " + trail_at_one_side.get() );
            break;
          case "with_positive_flat":
            with_positive_flat.set(config[1].equals("true"));
            SharedProps.print(config[0] + " set to: " + with_positive_flat.get() );
            break;


          // int
          case "equity_gain_change_count":
            equity_gain_change_count.set(Integer.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + equity_gain_change_count.get() );
            break;


          // long
          case "timezone_offset":
            timezone_offset.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + timezone_offset.get() );
            break;
          case "execute_inst_ms":
            execute_inst_ms.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + execute_inst_ms.get() );
            break;
          case "execute_inst_delay_ms":
            execute_inst_delay_ms.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + execute_inst_delay_ms.get() );
            break;
          case "execute_inst_delay_init_ms":
            execute_inst_delay_init_ms.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + execute_inst_delay_init_ms.get() );
            break;
          case "execute_inst_skip_weekend_secs":
            execute_inst_skip_weekend_secs.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + execute_inst_skip_weekend_secs.get() );
            break;
          case "execute_inst_skip_day_end_secs":
            execute_inst_skip_day_end_secs.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + execute_inst_skip_day_end_secs.get() );
            break;
          case "execute_inst_skip_day_begin_secs":
            execute_inst_skip_day_begin_secs.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + execute_inst_skip_day_begin_secs.get() );
            break;
          case "ticks_history_secs":
            ticks_history_secs.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + ticks_history_secs.get() );
            break;
          case "ticks_history_ttl":
            ticks_history_ttl.set(Long.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + ticks_history_ttl.get() );
            break;


          // double
          case "trail_step_1st_min":
            trail_step_1st_min.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + trail_step_1st_min.get() );
            break;
          case "trail_step_1st_divider":
            trail_step_1st_divider.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + trail_step_1st_divider.get() );
            break;
          case "trail_step_entry_multiplier":
            trail_step_entry_multiplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + trail_step_entry_multiplier.get() );
            break;
          case "trail_step_rest_plus_gain":
            trail_step_rest_plus_gain.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + trail_step_rest_plus_gain.get() );
            break;
          case "open_followup_amount_muliplier":
            open_followup_amount_muliplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + open_followup_amount_muliplier.get() );
            break;
          case "open_followup_step_offers_diff_devider":
            open_followup_step_offers_diff_devider.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + open_followup_step_offers_diff_devider.get() );
            break;
          case "open_followup_step_first_muliplier":
            open_followup_step_first_muliplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + open_followup_step_first_muliplier.get() );
            break;
          case "open_followup_step_first_upto_ratio":
            open_followup_step_first_upto_ratio.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + open_followup_step_first_upto_ratio.get() );
            break;
          case "open_followup_step_muliplier":
            open_followup_step_muliplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + open_followup_step_muliplier.get() );
            break;
          case "open_followup_require_growth_rate":
            open_followup_require_growth_rate.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + open_followup_require_growth_rate.get() );
            break;
          case "open_followup_flat_amt_muliplier":
            open_followup_flat_amt_muliplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + open_followup_flat_amt_muliplier.get() );
            break;
          case "flat_positive_amt_ratio":
            flat_positive_amt_ratio.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + flat_positive_amt_ratio.get() );
            break;
          case "flat_positive_profit_part":
            flat_positive_profit_part.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + flat_positive_profit_part.get() );
            break;
          case "order_zero_base_step_multiplier":
            order_zero_base_step_multiplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + order_zero_base_step_multiplier.get() );
            break;
          case "equity_gain_change":
            equity_gain_change.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + equity_gain_change.get() );
            break;
          case "amount":
            amount.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + amount.get() );
            break;
          case "merge_close_neg_side_multiplier":
            merge_close_neg_side_multiplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + merge_close_neg_side_multiplier.get() );
            break;
          case "merge_close_neg_side_growth_rate":
            merge_close_neg_side_growth_rate.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + merge_close_neg_side_growth_rate.get() );
            break;
          case "equity_gain_state_close_step_multiplier":
            equity_gain_state_close_step_multiplier.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + equity_gain_state_close_step_multiplier.get() );
            break;
          case "email_equity_gain_change":
            email_equity_gain_change.set(Double.valueOf(config[1]));
            SharedProps.print(config[0] + " set to: " + email_equity_gain_change.get() );
            break;


          // string
          case "reportEmail":
            reportEmail.set(config[1]);
            SharedProps.print(config[0] + " set to: " + reportEmail.get());
            break;
          case "reportName":
            reportName.set(config[1]);
            SharedProps.print(config[0] + " set to: " + reportName.get());
            break;

          default : //Optional
            //Statements
        }
      }
      fr.close();

    } catch (Exception e) {
      SharedProps.print("getConfigs E: " + e);
    }
  }

  public synchronized long delays_init_execution(long ts) {
    long delay = last_execution_ts.get() - ts;
    if(delay >= 0)
      return delay;
    last_execution_ts.set(
      ts +
      (Double.compare(SharedProps.get_leverage_used(), 10.0) < 0 ? 1000 : execute_inst_delay_init_ms.get())
    );
    return -1;
  }

  public void set_equity_gain_base(double v) {
    SharedProps.print("equity_gain_base chg: from " + equity_gain_base.get() + " to " + v);
    equity_gain_base.set(v);
  }

  public boolean is_email_report_enabled() {
    return !reportEmail.isEmpty();
  }



  public static int period_to_minutes(Period period){
    if(period == Period.ONE_HOUR) return (int) 60;
    else if(period == Period.DAILY) return (int) 60*24;
    else if(period == Period.FOUR_HOURS) return (int) 60*4;
    else if(period == Period.THIRTY_MINS) return (int) 30;
    else if(period == Period.FIFTEEN_MINS ) return (int) 15;
    else if(period == Period.WEEKLY ) return (int) 60*24*7;
    return (int) 2;
  }
  public static int minutes_to_period_scale(double timePeriod, double base){
    if(Double.compare(timePeriod,0.017) < 0)    timePeriod=0.017;

    if(Double.compare(timePeriod,base/60) <= 0)    return (int) timePeriod*60;
    if(Double.compare(timePeriod,base/6) <= 0)     return (int) timePeriod*6;
    if(Double.compare(timePeriod,base) <= 0)       return (int) timePeriod;
    if(Double.compare(timePeriod,base*5) <= 0)     return (int)(timePeriod/5);
    if(Double.compare(timePeriod,base*10) <= 0)    return (int)(timePeriod/10);
    if(Double.compare(timePeriod,base*15) <= 0)    return (int)(timePeriod/15);
    if(Double.compare(timePeriod,base*30) <= 0)    return (int)(timePeriod/30);
    if(Double.compare(timePeriod,base*60) <= 0)    return (int)(timePeriod/60);
    if(Double.compare(timePeriod,base*360) <= 0)   return (int)(timePeriod/360);
    if(Double.compare(timePeriod,base*1440) <= 0)  return (int)(timePeriod/1440);
    if(Double.compare(timePeriod,base*10080) <= 0) return (int)(timePeriod/10080);
    if(Double.compare(timePeriod,base*40320) <= 0) return (int)(timePeriod/40320);
    return (int) timePeriod;
  }
  public static int minutes_to_period_scale(double timePeriod){
    return minutes_to_period_scale(timePeriod, 1000);
  }
  public static Period minutes_to_period(double timePeriod, double base){
    if(Double.compare(timePeriod,base/60) <= 0)    return Period.ONE_SEC;
    if(Double.compare(timePeriod,base/6) <= 0)     return Period.TEN_SECS;
    if(Double.compare(timePeriod,base) <= 0)       return Period.ONE_MIN;
    if(Double.compare(timePeriod,base*5) <= 0)     return Period.FIVE_MINS;
    if(Double.compare(timePeriod,base*10) <= 0)    return Period.TEN_MINS;
    if(Double.compare(timePeriod,base*15) <= 0)    return Period.FIFTEEN_MINS;
    if(Double.compare(timePeriod,base*30) <= 0)    return Period.THIRTY_MINS;
    if(Double.compare(timePeriod,base*60) <= 0)    return Period.ONE_HOUR;
    if(Double.compare(timePeriod,base*360) <= 0)   return Period.FOUR_HOURS;
    if(Double.compare(timePeriod,base*1440) <= 0)  return Period.DAILY;
    if(Double.compare(timePeriod,base*10080) <= 0) return Period.WEEKLY;
    if(Double.compare(timePeriod,base*40320) <= 0) return Period.MONTHLY;
    return Period.FIFTEEN_MINS;
  }
  public static Period minutes_to_period(double timePeriod) {
    return minutes_to_period(timePeriod, 1000);
  }


  public static double get_inst_amt_min(Instrument inst) {
    double amt_min = inst.getTradeAmountIncrement();
    amt_min /= 1000;
    if(inst.getGroup().isCrypto()) {
      amt_min /= 1000;
      amt_min *= (inst.getMinTradeAmount() / inst.getTradeAmountIncrement());
    }
    return amt_min;
  }

  public static int get_double_scale(double v) {
    if(Double.compare(v, 1.0) >= 0)
      return 0;
    if(Double.compare(v, 0.1) >= 0)
      return 1;
    if(Double.compare(v, 0.01) >= 0)
      return 2;
    if(Double.compare(v, 0.001) >= 0)
      return 3;
    if(Double.compare(v, 0.0001) >= 0)
      return 4;
    if(Double.compare(v, 0.00001) >= 0)
      return 5;
    if(Double.compare(v, 0.000001) >= 0)
      return 6;
    if(Double.compare(v, 0.0000001) >= 0)
      return 7;
    if(Double.compare(v, 0.00000001) >= 0)
      return 8;
    if(Double.compare(v, 0.000000001) >= 0)
      return 9;
    if(Double.compare(v, 0.0000000001) >= 0)
      return 10;
    if(Double.compare(v, 0.00000000001) >= 0)
      return 11;
    return 12;
  }

}
