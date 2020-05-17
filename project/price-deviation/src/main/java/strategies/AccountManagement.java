// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.*;
import com.dukascopy.api.system.IClient;

import org.apache.commons.lang3.ObjectUtils.Null;

import com.dukascopy.api.IEngine.OrderCommand;
import direct.thither.lib.api.fms.FmsClient;
import direct.thither.lib.api.fms.FmsRspSetStats;
import direct.thither.lib.api.fms.FmsSetStatsItem;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AccountManagement implements IStrategy{

  public boolean stop_run = false;
  public boolean pid_restart = false;

  private StrategyConfigs configs;
  public IClient client;
  public long strategyId;
  
  private IEngine engine;
  private IHistory history;
  private IContext context;
  private IDataService dataService;
  private IAccount account;

  private FmsClient fms_client;

  public static ConcurrentLinkedQueue<FmsSetStatsItem> eq_state = new ConcurrentLinkedQueue<>();
  private TimeZone tz = TimeZone.getTimeZone("UTC");

  private int gain_close_count=0;

  @Override
  public void onStart(IContext ctx) {
    SharedProps.print("onStart start");
    configs = SharedProps.configs;
    configs.getConfigs();

    context = ctx;
    engine = ctx.getEngine();
    history = ctx.getHistory();
    dataService = ctx.getDataService();

    account = ctx.getAccount();
    configs.account_currency = account.getAccountCurrency();
    if(configs.fms_active) {
      fms_client = new FmsClient(configs.fms_id);
      fms_client.set_pass_phrase(configs.fms_pass_phrase);
      fms_client.set_keep_alive(true);
    }
    new Thread(new Runnable() {
      @Override
      public void run() {bg_tasks();
      }
    }).start();
  }

  private long on_acc_ts=SharedProps.get_sys_ts();
  @Override
  public void onAccount(IAccount acc) {
    if(SharedProps.get_ts()-on_acc_ts < 0)
      return;
    try {
      Double eq = account.getEquity();
      if(Double.isNaN(eq)) return;

      SharedProps.amount_balanced_by_margin_reached = Double.compare(
        configs.amount_balanced_from_margin, account.getUseOfLeverage()) < 0;

      double amount = 1;
      if(configs.onlyOneOrder) {
        amount = (eq-configs.amount_bonus)/configs.onlyOneOrderAmount;
      } else{
        int counted_inst = StrategyConfigs.instruments.length;
        if(counted_inst>0) {
          amount = ((eq-configs.amount_bonus) / (configs.amount_value_fixed*2))
              /(counted_inst*configs.merge_max*configs.num_orders_an_inst);
            // -account.getUsedMargin()
        }
      }
      amount /= 1000;
      if(Double.compare(amount, 0.001) < 0) amount = 0.001;
      else if(Double.compare(amount, 10) > 0) amount = 10;
      configs.set_amount(SharedProps.round(amount, 6));
      on_acc_ts = SharedProps.get_ts()+10000;

      if(configs.fms_active)
        eq_state.add(
          new FmsSetStatsItem(
            configs.fms_metric_id, Calendar.getInstance(tz).getTimeInMillis(), eq.longValue()));
    } catch (Exception e) {
      SharedProps.print("onAccount E: "+e.getMessage()+" Thread: " + Thread.currentThread().getName() + " " + e);
    }
    on_acc_ts = SharedProps.get_ts()+5000;
  }

  //
  private void bg_tasks() {
    setOfflineTime();
    int at_hour = -1;
    long five_minutes_timer = 0;
    long one_hour_timer = 0;
    long one_minute_timer = 0;
    long ten_secs_timer = 0;
    while(!stop_run){
      try{
        pid_restart=false;
        if(at_hour != Calendar.getInstance(TimeZone.getTimeZone("GMT")).get(Calendar.HOUR_OF_DAY)){
          at_hour = Calendar.getInstance(TimeZone.getTimeZone("GMT")).get(Calendar.HOUR_OF_DAY);
          SharedProps.print("setOfflineTime: "+Calendar.getInstance(
              TimeZone.getTimeZone("GMT")).get(Calendar.HOUR_OF_DAY));
          setOfflineTime();
        }
        if(SharedProps.offline_sleep!=0){
          SharedProps.print("bg_tasks sleep: "+SharedProps.offline_sleep);
          Thread.sleep(1000);
          commit_log();
          Thread.sleep(SharedProps.offline_sleep-1000);
          continue;
        }
        Thread.sleep(5000);
        if(SharedProps.get_sys_ts() - one_minute_timer > 60000) {
          configs.getConfigs();
          setGainBase();
          closeOrdersOnProfit();
          one_minute_timer = SharedProps.get_sys_ts();
        }
        if(SharedProps.get_sys_ts() - five_minutes_timer > 300000) {
          setInstruments();
          freeBusyTPSL();
          five_minutes_timer = SharedProps.get_sys_ts();
          commit_metrics();
        }
        //if(SharedProps.get_sys_ts() - one_hour_timer > 3600000) {
         // setInstrumentsAmountRatio();
        //}
        if(SharedProps.get_sys_ts() - ten_secs_timer > 10000) {
          SharedProps.print("bg_tasks");
          commit_log();
          ten_secs_timer = SharedProps.get_sys_ts();
        }
        send_mail_status();
      } catch (Exception e) {
        SharedProps.print("bg_tasks E: "+e.getMessage()+" Thread: "+Thread.currentThread().getName()+" "+e);
      }
    }
  }

  private void setOfflineTime(){
    try {
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      if(dataService.isOfflineTime(cal.getTimeInMillis()+60000))
        SharedProps.offline_sleep=(61-cal.get(Calendar.MINUTE))*60*1000;
      else {
        SharedProps.offline_sleep = 0;
        for(Instrument inst :StrategyConfigs.instruments)
          SharedProps.inst_active.put(inst.toString(), true);
      }
    }catch (Exception e){}
  }
  private void freeBusyTPSL(){
    try {
      for (String k : SharedProps.oBusy.keySet()) {
        if(SharedProps.get_sys_ts()-SharedProps.oBusy.get(k) > 12*60*60*1000)
          SharedProps.oBusy.remove(k);
      }
    } catch (Exception e) {
      SharedProps.print("freeBusyTPSL E: "+e.getMessage()+" Thread: " + Thread.currentThread().getName() + " " + e);
    }
  }

  private void setGainBase() {
    try {
      double eq;
      File file=new File(SharedProps.strategy_dir+"status.dat");
      if(file.exists()){
        FileReader fr = new FileReader(file);
        BufferedReader reader  = new BufferedReader(fr);
        String line = reader.readLine();
        fr.close();
        if(line !=null){
          SharedProps.print("Current Gain Base: "+ line);
          eq = Double.valueOf(line);
          if(!Double.isNaN(eq) && Double.compare(eq, configs.get_gainBase()) > 0)
            configs.set_gainBase(eq);
          else {
            FileWriter writer = new FileWriter(file);
            writer.write(Double.toString(SharedProps.round(configs.get_gainBase(),0)));
            writer.flush();
            writer.close();
          }
        }
      } else {
        double eq_unsettled = account.getEquity();
        ProfitLoss pl = getOrdersProfit();
        if(Double.isNaN(eq_unsettled) || pl==null) return;

        eq = eq_unsettled-pl.profit-configs.amount_bonus;

        FileWriter writer = new FileWriter(file);
        writer.write(Double.toString(SharedProps.round(eq,0)));
        writer.flush();
        writer.close();
        configs.set_gainBase(eq);
      }
    } catch (Exception e) {
      SharedProps.print("setGainBase E: "+e.getMessage()+" Thread: "+Thread.currentThread().getName()+" "+e);
    }
  }

  private void setInstruments (){
    try {
      for( Instrument inst : StrategyConfigs.instruments  ) {
        context.setSubscribedInstruments(java.util.Collections.singleton(inst), true);
      }
    } catch (Exception e) {
      SharedProps.print("setInstruments numInst E: "+e.getMessage()+" Thread: " + Thread.currentThread().getName() + " " + e);
    }
  }

  private void setInstrumentsAmountRatio(){
    double r;
    for( Instrument inst : StrategyConfigs.instruments  ) {
      if(!SharedProps.inst_active.get(inst.toString())) continue;
      try {
        //SharedProps.print("AmountRatio "+inst.toString()+
        //        " "+(context.getUtils().convertPipToCurrency(inst, configs.account_currency)*10000));
        r = 1/(context.getUtils().convertPipToCurrency(inst, configs.account_currency)*10000);
        r = SharedProps.round(r, 9);
        if(Double.compare(r, 50)> 0) r = 10;
        SharedProps.inst_amt_ratio.put(inst.toString(),r);
      }catch (Exception e){}
    }
  }


  //

  private boolean dayEndClose = false;
  private void closeOrdersOnProfit() {
    try {
      double eq_unsettled = account.getEquity();
      // double eq_unsettled = account.get();
      ProfitLoss pl = getOrdersProfit();
      double gainbase = configs.get_gainBase();
      //SharedProps.print("closeOrdersOnProfit: "+eq_unsettled+"-"+orders_profit+">"+gainbase);
      
      if(Double.isNaN(eq_unsettled) || pl == null || Double.compare(gainbase, 0) <= 0)
        return;
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

      eq_unsettled -= configs.amount_bonus;
      double eq = eq_unsettled-pl.profit;

      if(Double.compare(eq, gainbase*configs.gain_close_constant_percent) > 0
          && Double.compare(eq, gainbase+(-configs.gain_close_constant_neg_portion*pl.loss)) > 0) {
        SharedProps.print("closeOrdersOnProfit constant"+
            " eq:"+eq+" > gainbase:"+gainbase+"*"+configs.gain_close_constant_percent+
            " eq:"+eq+" > gainbase:"+gainbase+"+"+(-configs.gain_close_constant_neg_portion*pl.loss)+
            " c:"+(gain_close_count+1));
        closeOrders("constant");

      } else if(Double.compare(eq, gainbase*configs.gain_close_day_end_percent) > 0
          && Double.compare(eq, gainbase+(-configs.gain_close_day_end_neg_portion*pl.loss)) > 0
          && cal.get(Calendar.HOUR_OF_DAY) == 23 && cal.get(Calendar.MINUTE) >= 30 && !dayEndClose) {
        SharedProps.print("closeOrdersOnProfit dayEnd"+
            " eq:"+eq+" > gainbase:"+gainbase+"*"+configs.gain_close_day_end_percent+
            " eq:"+eq+" > gainbase:"+gainbase+"+"+(-configs.gain_close_day_end_neg_portion*pl.loss)+
            " c:"+(gain_close_count+1));
        closeOrders("dayEnd");

      } else if(Double.compare(eq, gainbase*configs.gain_close_wknd_percent) > 0
          && Double.compare(eq, gainbase+(-configs.gain_close_wknd_neg_portion*pl.loss)) > 0
          && cal.get(Calendar.DAY_OF_WEEK) == 5 && cal.get(Calendar.HOUR_OF_DAY) >= 18) {
        SharedProps.print("closeOrdersOnProfit weekend"+
            " eq:"+eq+" > gainbase:"+gainbase+"*"+configs.gain_close_wknd_percent+
            " eq:"+eq+" > gainbase:"+gainbase+"+"+(-configs.gain_close_wknd_neg_portion*pl.loss)+
            " c:"+(gain_close_count+1));
        closeOrders("weekend");
        if(cal.get(Calendar.HOUR_OF_DAY) >= 23)
          for(Instrument inst :StrategyConfigs.instruments)
            SharedProps.inst_active.put(inst.toString(), false);

      }else if(Double.compare(eq_unsettled, gainbase*configs.gain_close_all_percent) > 0) {
        SharedProps.print("closeOrdersOnProfit CloseAll" +
            " eq_unsettled:"+eq_unsettled+" > gainbase:"+gainbase+"*"+configs.gain_close_all_percent+
            " c:"+(gain_close_count+1));
        closeOrders("CloseAll");
      }else 
        gain_close_count=0;

      if(dayEndClose && cal.get(Calendar.HOUR_OF_DAY) >= 0 && cal.get(Calendar.MINUTE) >= 0)
        dayEndClose = false;

    } catch (Exception e) {
      SharedProps.print("closeOrdersOnProfit E: "+e.getMessage()+" "+
                        "Thread: "+Thread.currentThread().getName()+" " + e);
    }
  }
  private void closeOrders(String close_type) {
    gain_close_count+=1;
    if(gain_close_count<configs.gain_close_count)
      return;
    gain_close_count=0;

    switch (close_type){
      case "dayEnd":
        dayEndClose = true;
        break;
    }

    try {
      for (IOrder o : engine.getOrders()) {
        if (o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED)
          continue;
        if (o.getOrderCommand() != OrderCommand.SELL && o.getOrderCommand() != OrderCommand.BUY)
          continue;
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 5000);
      }
      Thread.sleep(3000);
    } catch (Exception e) {
      SharedProps.print("closeOrders E: "+e.getMessage()+" "+
                        "Thread: "+Thread.currentThread().getName()+" " + e);
    }
    context.executeTask(new CloseOrders(close_type));
  }
  private class CloseOrders implements Callable<CloseOrders> {
    private final String close_type;

    public CloseOrders(String s) {
      close_type = s;
    }

    public CloseOrders call() {
      try {
        OrderCommand cmd;
        Instrument inst;
        double pip_lvl, std_dev;
        for (IOrder o : engine.getOrders()) {
          if (o.getLabel().contains("Signal:") && !configs.manageSignals)
            continue;
          if (o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED)
            continue;
          cmd = o.getOrderCommand();
          if (cmd != OrderCommand.SELL && cmd != OrderCommand.BUY)
            continue;

          inst = o.getInstrument();
          std_dev = getStdDevPip(inst);
          if(Double.isNaN(std_dev)) continue;

          pip_lvl = Double.NaN;
          switch (close_type){
            case "constant":
              pip_lvl = SharedProps.round(std_dev/configs.gain_close_constant_std_dev_divider, 1);
              if (Double.compare(configs.gain_close_constant_pip, pip_lvl) > 0)
                pip_lvl = configs.gain_close_constant_pip;
              break;
            case "dayEnd":
              pip_lvl = SharedProps.round(std_dev/configs.gain_close_day_end_std_dev_divider, 1);
              if (Double.compare(configs.gain_close_day_end_pip, pip_lvl) > 0)
                pip_lvl = configs.gain_close_day_end_pip;
              break;
            case "weekend":
              pip_lvl = SharedProps.round(std_dev/configs.gain_close_wknd_std_dev_divider, 1);
              if (Double.compare(configs.gain_close_wknd_pip, pip_lvl) > 0)
                pip_lvl = configs.gain_close_wknd_pip;
              break;
            case "CloseAll":
              pip_lvl = configs.gain_close_all_pip;
              break;
          }
          if(Double.isNaN(pip_lvl)) continue;

          if (Double.compare(o.getProfitLossInPips(), -pip_lvl) < 0) {
            SharedProps.print("CloseOrders "+close_type+": "
                +inst.toString()+" "+cmd+" "+o.getProfitLossInPips()+"<"+(-pip_lvl));
            o.close();
          }
        }

        double eq_unsettled = account.getEquity();
        ProfitLoss pl = getOrdersProfit();
        if(!Double.isNaN(eq_unsettled) && pl != null) {
          double eq = eq_unsettled - pl.profit - configs.amount_bonus;
          if (Double.compare(eq, configs.get_gainBase()) > 0)
            configs.set_gainBase(eq);
        }
      } catch (Exception e) {
        SharedProps.print("CloseOrders call() E: " + e.getMessage() + " " +
                          "Thread: " + Thread.currentThread().getName() + " " + e);
      }
      return this;
    }
  }

  //
  class ProfitLoss {
    public double profit = 0;
    public double loss = 0;
  }
  private ProfitLoss getOrdersProfit() {
    ProfitLoss profit_loss = new ProfitLoss();

    double add_profit = Double.NaN;
    double add_loss = Double.NaN;
    double inst_pip_v, open_price, sl_price, pip_acc_v;
    Instrument inst;
    try {
      for( IOrder p : engine.getOrders()) {
        if(p.getState() != IOrder.State.FILLED && p.getState() != IOrder.State.OPENED)
          continue;
        OrderCommand cmd = p.getOrderCommand();
        if (cmd != OrderCommand.SELL && cmd != OrderCommand.BUY)
          continue;
        if(Double.compare(p.getProfitLossInPips(), 0) <= 0) {
          add_loss = p.getProfitLossInUSD();
          if(Double.isNaN(add_loss)) return null;
          profit_loss.loss += add_loss;
          continue;
        }
        if(Double.compare(p.getStopLossPrice(), 0) == 0){
          if(Double.compare(p.getProfitLossInUSD(), 0) >= 0)
            add_profit = p.getProfitLossInUSD();
            if(Double.isNaN(add_profit)) return null;
            profit_loss.profit += add_profit;
          continue;
        }

        inst = p.getInstrument();
        inst_pip_v = inst.getPipValue();
        open_price= p.getOpenPrice();
        sl_price= p.getStopLossPrice();
        pip_acc_v = p.getProfitLossInUSD()/p.getProfitLossInPips();

        if(cmd == OrderCommand.BUY && open_price < sl_price){
          add_profit = ((getLastTick(inst).getBid()-sl_price)/inst_pip_v)*pip_acc_v;
          if(Double.isNaN(add_profit)) return null;
          profit_loss.profit += add_profit;
        }else if (cmd == OrderCommand.SELL && open_price > sl_price){
          add_profit = ((sl_price-getLastTick(inst).getAsk())/inst_pip_v)*pip_acc_v;
          if(Double.isNaN(add_profit)) return null;
          profit_loss.profit += add_profit;
        }
      }
    } catch (Exception e) {
      SharedProps.print("getOrdersProfit E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e);
    }
    return profit_loss;
  }

  private ITick getLastTick(Instrument instrument) {
    ITick tick;
    for(int i=0;i<3;i++){
      try{
        tick = history.getLastTick(instrument);
        if(tick!=null) return tick;
        Thread.sleep(1);
      } catch (Exception e){}
    }
    return null;
  }

  private double getStdDevPip(Instrument inst) {
    return SharedProps.inst_std_dev_avg.get(inst.toString())/inst.getPipValue();
  }

  //
  private void commit_metrics(){
    if(!configs.fms_active || eq_state.isEmpty()) return;
    try {
      List<FmsSetStatsItem> items = new ArrayList<>();
      while (!eq_state.isEmpty()) items.add(eq_state.poll());
      FmsRspSetStats rsp = fms_client.push_list(items);

      SharedProps.log_q.add(rsp+", "+(rsp.succeed == items.size()));
    }catch (Exception e){
      SharedProps.log_q.add("commit_metrics: "+e.getMessage());
      System.out.print(e.getMessage());
    }
  }

  private int email_hour_c=0;
  private int email_hour=25;

  private void send_mail_status() {
    if(!configs.emailReports || SharedProps.reports_q.isEmpty()) return;

    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    if(email_hour_c == 100) {
      if(email_hour == cal.get(Calendar.HOUR_OF_DAY))
        return;
      email_hour_c=0;
      email_hour = cal.get(Calendar.HOUR_OF_DAY);
    }
    try{
      String content = "";
      content += cal.get(Calendar.DAY_OF_WEEK)+" "+
          cal.get(Calendar.HOUR_OF_DAY)+":"+cal.get(Calendar.MINUTE)+SharedProps.newLine;
      content += "Eq: "+account.getEquity()+SharedProps.newLine;
      content += "Open: "+engine.getOrders().size()+SharedProps.newLine;

      while (!SharedProps.reports_q.isEmpty()) {
        if(content.length()+SharedProps.reports_q.peek().length() > 500)
          break;
        content += SharedProps.reports_q.poll();
        try{Thread.sleep(300);}catch (Exception e){}
      }

      context.getUtils().sendMail("kashirin.alex@gmail.com", configs.reportName+" acc status", content);
      SharedProps.print("send_mail_status content: "+content);
      email_hour_c++;

    } catch (Exception e) {
      SharedProps.print("sendMail E: "+e.getMessage());
    }
  }

  private String log_file = "";
  private FileWriter log_fd = null;
  private void commit_log(){
    if(SharedProps.log_q.isEmpty()) return;

    try {
      Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
      String day = cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DATE);
      if(!log_file.equals(day)){
        if(log_fd!=null){
          log_fd.close();
          log_fd = null;
        }
        log_file = day;
      }
      if(log_fd==null)
        log_fd = new FileWriter(SharedProps.strategy_dir+"logs/"+log_file+".log");

      while (!SharedProps.log_q.isEmpty())
        log_fd.append(SharedProps.log_q.poll()+"\n");

      log_fd.flush();
    }catch (Exception e){
      System.out.print(e.getMessage());
    }
  }

  @Override
  public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) {}
  @Override
  public void onTick(Instrument instrument, ITick tick) {}
  @Override
  public void onMessage(IMessage message) {}
  @Override
  public void onStop(){
    stop_run = true;
  }
}
