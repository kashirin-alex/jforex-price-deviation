// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.Instrument;
import com.dukascopy.api.ICurrency;
import com.dukascopy.api.Configurable;
import com.dukascopy.api.Period;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;

public class StrategyConfigs {

  public ICurrency account_currency;

  static public Instrument[] instruments;

  static public Instrument[] getStrategyInstruments(){
    SharedProps.print("|"+SharedProps.strategy_dir+"instruments.conf|");
    File file=new File(SharedProps.strategy_dir+"instruments.conf");
    Instrument[] insts = {};
    if(!file.exists()) return insts;
    try {
      FileReader fr = new FileReader(file);
      BufferedReader reader  = new BufferedReader(fr);
      String line;
      while ((line = reader.readLine()) != null) {
        SharedProps.print("|"+line+"|");

        Instrument inst = Instrument.fromString(line);
        if(inst == null || !Instrument.contains(line))
          continue;
        insts = Arrays.copyOf(insts, insts.length + 1);
        insts[insts.length - 1] = inst;
        SharedProps.inst_active.put(inst.toString(), false);
      }
      fr.close();
      SharedProps.print("num instruments: "+insts.length+"|");
    } catch (Exception e) {
      SharedProps.print(e);
    }
    return insts;
  }

  @Configurable("Debug output")
  public boolean debug = false;

  public int slippage = 0;

  @Configurable("gain Base")
  public double gainBase = 0;

  @Configurable("Gain close required count")
  public int gain_close_count = 3;
  @Configurable("Gain close value grow")
  public double gain_close_value_grow = 10.00;
  @Configurable("Gain close amount devider")
  public double gain_close_amount_devider = 2.00;

  public double amount = 0.001;
  @Configurable("Fixed Acc.Currency value for 0.001 amt")
  public double amount_value_fixed = 15.00;

  @Configurable("Bonus amount transparent")
  public double amount_bonus = 0;

  @Configurable("Merge close negative-side step multiplier")
  public double merge_close_neg_side_multiplier = 5.00;

  
  @Configurable("Open follow up amount side multiplier")
  public double open_followup_amount_muliplier = 1.0;
  @Configurable("Open follow up require distance first-step")
  public double open_followup_step_first_muliplier = 6.50;
  @Configurable("Open follow up require distance by amount-difference")
  public double open_followup_step_muliplier = 7.20;
  @Configurable("Open follow up require distance growth-rate")
  public double open_followup_require_growth_rate = 0.10;

  @Configurable("Positive order at step multiplier")
  public double positive_order_step_multiplier = 0.00;

  @Configurable("trail step managed")
  public boolean trail_managed = false;
  @Configurable("trail only at inst on one side")
  public boolean trail_at_one_side = false;

  @Configurable("trail step min pip")
  public double trail_step_1st_min = 1.30;
  @Configurable("trail step 1st divider of min")
  public double trail_step_1st_divider = 2.00;
  @Configurable("trail step entry multiplier")
  public double trail_step_entry_multiplier = 4.0;
  @Configurable("trail step rest plus gain muliplier")
  public double trail_step_rest_plus_gain = 0.33;

  @Configurable("Execute inst check ms")
  public long execute_inst_ms = 80;

  @Configurable("email Reports")
  public boolean emailReports = false;
  @Configurable("Report Name")
  public String reportName = "";
  @Configurable("Report Email")
  public String reportEmail = "";


  private static long getConfigsFileChange = 0;
  private static long getConfigsTimer = 0;


  public synchronized void getConfigs() {
    try {
      if(getConfigsTimer > System.currentTimeMillis() - 60 * 1000) return;
      getConfigsTimer = System.currentTimeMillis();

      File file=new File(SharedProps.strategy_dir+"default.conf");
      if(!file.exists()) return;
      long fileLastModified = file.lastModified();
      if (getConfigsFileChange == fileLastModified)return;

      getConfigsFileChange = fileLastModified;
      FileReader fr = new FileReader(file);
      BufferedReader reader  = new BufferedReader(fr);

      String line;
      while ((line = reader.readLine()) != null) {
        if(line.length() == 0 || line.startsWith("#"))
          continue;
        SharedProps.print("setting='" + line + "'");

        String[] config = line.split(":");
        switch(config[0]) {

          case "debug":
            debug = config[1].equals("true");
            SharedProps.print("debug set to: "+debug);
            break;

          case "execute_inst_ms":
            execute_inst_ms = Long.valueOf(config[1]);
            SharedProps.print("execute_inst_ms set to: "+execute_inst_ms);
            break;

          case "trail_managed":
            trail_managed = config[1].equals("true");
            SharedProps.print("trail_managed set to: "+trail_managed);
            break;
          case "trail_at_one_side":
            trail_at_one_side = config[1].equals("true");
            SharedProps.print("trail_at_one_side set to: "+trail_at_one_side);
            break;
            
          case "trail_step_1st_min":
          trail_step_1st_min = Double.valueOf(config[1]);
            SharedProps.print("trail_step_1st_min set to: "+trail_step_1st_min);
            break;
          case "trail_step_1st_divider":
          trail_step_1st_divider = Double.valueOf(config[1]);
            SharedProps.print("trail_step_1st_divider set to: "+trail_step_1st_divider);
            break;
          case "trail_step_entry_multiplier":
            trail_step_entry_multiplier = Double.valueOf(config[1]);
            SharedProps.print("trail_step_entry_multiplier set to: "+trail_step_entry_multiplier);
            break;

          case "trail_step_rest_plus_gain":
          trail_step_rest_plus_gain = Double.valueOf(config[1]);
            SharedProps.print("trail_step_rest_plus_gain set to: "+trail_step_rest_plus_gain);
            break;
    
          case "open_followup_amount_muliplier":
            open_followup_amount_muliplier = Double.valueOf(config[1]);
            SharedProps.print("open_followup_amount_muliplier set to: "+open_followup_amount_muliplier);
            break;
          case "open_followup_step_first_muliplier":
            open_followup_step_first_muliplier = Double.valueOf(config[1]);
            SharedProps.print("open_followup_step_first_muliplier set to: "+open_followup_step_first_muliplier);
            break;
          case "open_followup_step_muliplier":
            open_followup_step_muliplier = Double.valueOf(config[1]);
            SharedProps.print("open_followup_step_muliplier set to: "+open_followup_step_muliplier);
            break;
          case "open_followup_require_growth_rate":
            open_followup_require_growth_rate = Double.valueOf(config[1]);
            SharedProps.print("open_followup_require_growth_rate set to: "+open_followup_require_growth_rate);
            break;
          case "positive_order_step_multiplier":
            positive_order_step_multiplier = Double.valueOf(config[1]);
            SharedProps.print("positive_order_step_multiplier set to: "+positive_order_step_multiplier);
            break;

          case "gain_close_count":
            gain_close_count = Integer.valueOf(config[1]);
            SharedProps.print("gain_close_count set to: "+gain_close_count);
            break;
          case "gain_close_value_grow":
            gain_close_value_grow = Double.valueOf(config[1]);
            SharedProps.print("gain_close_value_grow set to: "+gain_close_value_grow);
            break;
          case "gain_close_amount_devider":
            gain_close_amount_devider = Double.valueOf(config[1]);
            SharedProps.print("gain_close_amount_devider set to: "+gain_close_amount_devider);
            break;

          case "amount_value_fixed":
            amount_value_fixed = Double.valueOf(config[1]);
            SharedProps.print("amount_value_fixed set to: "+amount_value_fixed);
            break;

          case "merge_close_neg_side_multiplier":
            merge_close_neg_side_multiplier = Double.valueOf(config[1]);
            SharedProps.print("merge_close_neg_side_multiplier set to: "+merge_close_neg_side_multiplier);
            break;
          case "amount_bonus":
            amount_bonus = Double.valueOf(config[1]);
            SharedProps.print("amount_bonus set to: "+amount_bonus);
            break;

          case "emailReports":
            emailReports = config[1].equals("true");
            SharedProps.print("emailReports set to: "+emailReports);
            break;

          case "reportName":
            reportName = config[1];
            SharedProps.print("reportName set to: "+reportName);
            break;

          case "reportEmail":
            reportEmail = config[1];
            SharedProps.print("reportEmail set to: "+reportEmail);
            break;

          default : //Optional
            //Statements
        }
      }
      fr.close();

    } catch (Exception e) {
      SharedProps.print("getConfig E: "+e.getMessage()+ " " + e);
    }
  }



  public int period_to_minutes(Period period){
    if(period == Period.ONE_HOUR) return (int) 60;
    else if(period == Period.DAILY) return (int) 60*24;
    else if(period == Period.FOUR_HOURS) return (int) 60*4;
    else if(period == Period.THIRTY_MINS) return (int) 30;
    else if(period == Period.FIFTEEN_MINS ) return (int) 15;
    else if(period == Period.WEEKLY ) return (int) 60*24*7;
    return (int) 2;
  }

  public int minutes_to_period_scale(double timePeriod, double base){
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
  public int minutes_to_period_scale(double timePeriod){
    return minutes_to_period_scale(timePeriod, 1000);
  }

  public Period minutes_to_period(double timePeriod, double base){
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
  public Period minutes_to_period(double timePeriod){
    return minutes_to_period(timePeriod, 1000);
  }

  public synchronized double get_gainBase(){
    return gainBase;
  }
  public synchronized void set_gainBase(double v){
    SharedProps.print("gainbase chg: from "+gainBase+" to "+v);
    gainBase = v;
  }
  public synchronized double get_amount(){
    return amount;
  }
  public synchronized void set_amount(double v){
    amount = v;
  }
}
