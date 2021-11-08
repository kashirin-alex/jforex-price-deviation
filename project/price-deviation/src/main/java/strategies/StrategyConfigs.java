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
  @Configurable("Gain close on over-loss percentage")
  public double gain_close_overloss_percent = 1/3;
  @Configurable("Gain close on over-loss at least percentage value gain")
  public double gain_close_overloss_atleast_percent = 1.0005;
  @Configurable("Gain close on over-loss close amount")
  public double gain_close_overloss_close_ratio = 0.50;

  @Configurable("Gain close fixed applied at above leverage")
  public double gain_close_fixed_from_leverage = 15;
  @Configurable("Gain close fixed percentage above gain-base")
  public double gain_close_fixed_percent = 1.05;

  public double amount = 0.001;
  @Configurable("Fixed Acc.Currency value for 0.001 amt")
  public double amount_value_fixed = 2;

  @Configurable("Bonus amount transparent")
  public double amount_bonus = 0;

  @Configurable("Merge max")
  public double merge_max = 10;
  @Configurable("Merge distance StdDev divider")
  public double merge_distance_std_dev_divider = 3;
  @Configurable("Merge followup step muliplier")
  public double merge_followup_step_muliplier = 2.25;
  @Configurable("Merge close negative-side step multiplier")
  public double merge_close_neg_side_multiplier = 1.5;

  @Configurable("Open new order at negative distance of StdDev divider")
  public double open_new_std_dev_divider = 5;
  @Configurable("Open follow up order at positive distance by step multiplier")
  public double open_followup_step_muliplier = 2.5;
  @Configurable("Open support side ration")
  public double open_support_side_ratio = 1.00;

  @Configurable("Profitable ratio min of StdDev on min step")
  public double profitable_ratio_min = 4;
  @Configurable("Profitable ratio max of StdDev on min step")
  public double profitable_ratio_max = 8;
  @Configurable("Profitable ratio chg from min to max")
  public double profitable_ratio_chg = 0.01;
  @Configurable("Profitable ratios good for bars in history")
  public int profitable_ratio_good_for_bars = 1;

  @Configurable("Standard deviation minutes timePeriod")
  public int std_dev_time = 168;
  @Configurable("Standard deviation Period")
  public Period std_dev_period = Period.ONE_HOUR;

  @Configurable("trail step min pip")
  public double trail_step_1st_min = 6.00;
  @Configurable("trail step 1st divider of min")
  public double trail_step_1st_divider = 2;
  @Configurable("trail step rest plus gain muliplier")
  public double trail_step_rest_plus_gain = 0.10;

  @Configurable("Price Difference multiplier")
  public double price_diff_multiplier = 0.90;

  @Configurable("Execute inst check ms")
  public long execute_inst_ms = 100;

  @Configurable("manage Signals")
  public boolean manageSignals = false;

  @Configurable("tick with different price")
  public boolean onlyDifferentTick = false;


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

          case "profitable_ratio_min":
            profitable_ratio_min = Double.valueOf(config[1]);
            SharedProps.print("profitable_ratio_min set to: "+profitable_ratio_min);
            break;
          case "profitable_ratio_max":
            profitable_ratio_max = Double.valueOf(config[1]);
            SharedProps.print("profitable_ratio_max set to: "+profitable_ratio_max);
            break;
          case "profitable_ratio_chg":
            profitable_ratio_chg = Double.valueOf(config[1]);
            SharedProps.print("profitable_ratio_chg set to: "+profitable_ratio_chg);
            break;
          case "profitable_ratio_good_for_bars":
            profitable_ratio_good_for_bars = Integer.valueOf(config[1]);
            SharedProps.print("profitable_ratio_good_for_bars set to: "+profitable_ratio_good_for_bars);
            break;

          case "std_dev_time":
            std_dev_time = Integer.valueOf(config[1]);
            SharedProps.print("std_dev_time set to: "+std_dev_time);
            break;


          case "trail_step_1st_min":
          trail_step_1st_min = Double.valueOf(config[1]);
            SharedProps.print("trail_step_1st_min set to: "+trail_step_1st_min);
            break;
          case "trail_step_1st_divider":
          trail_step_1st_divider = Double.valueOf(config[1]);
            SharedProps.print("trail_step_1st_divider set to: "+trail_step_1st_divider);
            break;

          case "trail_step_rest_plus_gain":
          trail_step_rest_plus_gain = Double.valueOf(config[1]);
            SharedProps.print("trail_step_rest_plus_gain set to: "+trail_step_rest_plus_gain);
            break;

          case "open_new_std_dev_divider":
            open_new_std_dev_divider = Double.valueOf(config[1]);
            SharedProps.print("open_new_std_dev_divider set to: "+open_new_std_dev_divider);
            break;
          case "open_followup_step_muliplier":
            open_followup_step_muliplier = Double.valueOf(config[1]);
            SharedProps.print("open_followup_step_muliplier set to: "+open_followup_step_muliplier);
            break;
          case "open_support_side_ratio":
            open_support_side_ratio = Double.valueOf(config[1]);
            SharedProps.print("open_support_side_ratio set to: "+open_support_side_ratio);
            break;

          case "gain_close_count":
            gain_close_count = Integer.valueOf(config[1]);
            SharedProps.print("gain_close_count set to: "+gain_close_count);
            break;

          case "gain_close_overloss_percent":
            gain_close_overloss_percent = Double.valueOf(config[1]);
            SharedProps.print("gain_close_overloss_percent set to: "+gain_close_overloss_percent);
            break;
          case "gain_close_overloss_atleast_percent":
            gain_close_overloss_atleast_percent = Double.valueOf(config[1]);
            SharedProps.print("gain_close_overloss_atleast_percent set to: "+gain_close_overloss_atleast_percent);
            break;
          case "gain_close_overloss_close_ratio":
            gain_close_overloss_close_ratio = Double.valueOf(config[1]);
            SharedProps.print("gain_close_overloss_close_ratio set to: "+gain_close_overloss_close_ratio);
            break;

          case "gain_close_fixed_from_leverage":
            gain_close_fixed_from_leverage = Double.valueOf(config[1]);
            SharedProps.print("gain_close_fixed_from_leverage set to: "+gain_close_fixed_from_leverage);
            break;
          case "gain_close_fixed_percent":
            gain_close_fixed_percent = Double.valueOf(config[1]);
            SharedProps.print("gain_close_fixed_percent set to: "+gain_close_fixed_percent);
            break;

          case "amount_value_fixed":
            amount_value_fixed = Double.valueOf(config[1]);
            SharedProps.print("amount_value_fixed set to: "+amount_value_fixed);
            break;

          case "merge_distance_std_dev_divider":
            merge_distance_std_dev_divider = Double.valueOf(config[1]);
            SharedProps.print("merge_distance_std_dev_divider set to: "+merge_distance_std_dev_divider);
            break;
          case "merge_followup_step_muliplier":
            merge_followup_step_muliplier = Double.valueOf(config[1]);
            SharedProps.print("merge_followup_step_muliplier set to: "+merge_followup_step_muliplier);
            break;
          case "merge_close_neg_side_multiplier":
            merge_close_neg_side_multiplier = Double.valueOf(config[1]);
            SharedProps.print("merge_close_neg_side_multiplier set to: "+merge_close_neg_side_multiplier);
            break;
          case "merge_max":
            merge_max = Double.valueOf(config[1]);
            SharedProps.print("merge_max set to: "+merge_max);
            break;

          case "amount_bonus":
            amount_bonus = Double.valueOf(config[1]);
            SharedProps.print("amount_bonus set to: "+amount_bonus);
            break;

          case "price_diff_multiplier":
            price_diff_multiplier = Double.valueOf(config[1]);
            SharedProps.print("price_diff_multiplier set to: "+price_diff_multiplier);
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
