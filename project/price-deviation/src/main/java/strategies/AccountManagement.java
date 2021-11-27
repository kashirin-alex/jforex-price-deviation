// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.*;
import com.dukascopy.api.system.IClient;
import com.dukascopy.api.IEngine.OrderCommand;

import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class AccountManagement implements IStrategy{

  public AtomicBoolean stop_run = new AtomicBoolean(false);
  public AtomicBoolean pid_restart = new AtomicBoolean(false);

  private StrategyConfigs configs;
  public IClient client;
  public long strategyId;
  
  private IEngine engine;
  private IHistory history;
  public IContext context;
  private IAccount account;

  private long on_acc_ts = SharedProps.get_sys_ts();

  private int gain_close_count=0;

  @Override
  public void onStart(IContext ctx) {
    SharedProps.print("onStart start");
    configs = SharedProps.configs;

    context = ctx;
    engine = ctx.getEngine();
    history = ctx.getHistory();

    account = ctx.getAccount();
    configs.account_currency = account.getAccountCurrency();

    new Thread(new Runnable() {
      @Override
      public void run() {bg_tasks();
      }
    }).start();
  }

  @Override
  public void onAccount(IAccount acc) {
    Double eq = account.getEquity();
    if(Double.isNaN(eq))
      return;
    if(SharedProps.get_ts()-on_acc_ts < 0)
      return;
    try {
      double amount = configs.amount_value_fixed;
      if(Double.compare(amount, 0.001) < 0)   amount = 0.001;
      else if(Double.compare(amount, 10) > 0) amount = 10;
      configs.set_amount(SharedProps.round(amount, 6));
      on_acc_ts = SharedProps.get_ts()+10000;
    } catch (Exception e) {
      SharedProps.print("onAccount E: "+e.getMessage()+" Thread: " + Thread.currentThread().getName() + " " + e);
    }
    on_acc_ts = SharedProps.get_ts()+5000;
  }

  @Override
  public void onMessage(IMessage message) {
    try {
      switch(message.getType()) {
        case INSTRUMENT_STATUS: 
          IInstrumentStatusMessage msg = (IInstrumentStatusMessage) message;
          Instrument inst = msg.getInstrument();
          Boolean at = SharedProps.inst_active.get(inst.toString());
          if(at == null || at != msg.isTradable()) {
            SharedProps.inst_active.put(inst.toString(), msg.isTradable());
            SharedProps.print("Changed " + inst.toString() + " isTradable status:" + msg.isTradable());
          }
          break;
        default:
          if(configs.debug)
            SharedProps.print("acc - Message: " + message.toString());
          break;
      }
    } catch (Exception e) {
      SharedProps.print("onAccount E: "+e.getMessage()+" Thread: " + Thread.currentThread().getName() + " " + e);
    }
  }

  //
  private void bg_tasks() {
    long five_minutes_timer = 0;
    long one_minute_timer = 0;
    long ten_secs_timer = 0;
    while(!stop_run.get()){
      commit_log();
      try{
        pid_restart.set(false);
        Thread.sleep(5000);
        if(SharedProps.get_sys_ts() - one_minute_timer > 60000) {
          configs.getConfigs();
          double gain = configs.get_gainBase();
          if(Double.isNaN(gain) || Double.compare(gain, 0.0) == 0)
            setGainBase();
          one_minute_timer = SharedProps.get_sys_ts();
        }
        if(SharedProps.get_sys_ts() - five_minutes_timer > 300000) {
          setInstruments();
          freeBusyTPSL();
          five_minutes_timer = SharedProps.get_sys_ts();
        }
        //if(SharedProps.get_sys_ts() - one_hour_timer > 3600000) {
         // setInstrumentsAmountRatio();
        //}
        if(SharedProps.get_sys_ts() - ten_secs_timer > 10000) {
          SharedProps.print("bg_tasks");
          closeOrdersOnProfit();
          ten_secs_timer = SharedProps.get_sys_ts();
        }
        send_mail_status();
      } catch (Exception e) {
        SharedProps.print(
          "bg_tasks E: "+e.getMessage()+
          " Thread: "+Thread.currentThread().getName()+" "+e);
      }
    }
  }

  private void freeBusyTPSL(){
    try {
      for (String k : SharedProps.oBusy.keySet()) {
        if(SharedProps.get_sys_ts() - SharedProps.oBusy.get(k) > 60*60*1000)
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
      if(configs.account_currency.getCurrencyCode().compareTo("ILS") == 0)
        context.setSubscribedInstruments(java.util.Collections.singleton(Instrument.USDILS), true);
    } catch (Exception e) {
      SharedProps.print("setInstruments numInst E: "+e.getMessage()+" Thread: " + Thread.currentThread().getName() + " " + e);
    }
  }


  //
  private void closeOrdersOnProfit() {
    try {
      
      double eq_unsettled = account.getEquity();
      if(Double.isNaN(eq_unsettled))
        return;

      ProfitLoss pl = getOrdersProfit();
      if(pl == null)
        return;

      double gainbase = configs.get_gainBase();
      if(Double.isNaN(gainbase) || Double.compare(gainbase, 0) <= 0)
        return;

      eq_unsettled -= configs.amount_bonus;
      double eq = eq_unsettled - pl.profit;
      if(Double.compare(eq, gainbase + configs.gain_close_value_grow) >= 0) {
        gain_close_count += 1;
        SharedProps.print("closeOrdersOnProfit eq=" + eq + " gainbase=" + gainbase + " c=" + gain_close_count);
        if(gain_close_count == configs.gain_close_count) {
          gain_close_count = 0;
          closeOrder();
        }
      } else {
        gain_close_count = 0;
      }

    } catch (Exception e) {
      SharedProps.print("closeOrdersOnProfit E: " + e.getMessage() + " Thread: "+Thread.currentThread().getName()+" " + e);
    }
  }

  private void apply_gainbase() {
    double eq_unsettled = account.getEquity();
    ProfitLoss pl = getOrdersProfit();
    if(!Double.isNaN(eq_unsettled) && pl != null) {
      double eq = eq_unsettled - pl.profit - configs.amount_bonus;
      if (Double.compare(eq, configs.get_gainBase()) > 0)
        configs.set_gainBase(eq);
    }
    setGainBase();
  }

  /// GAIN CLOSE
  private void closeOrder() {

    double open_price, sl_price;
    OrderCommand cmd;
    Instrument inst;
    boolean closing;        
    ITick tick; 
    double step, atleast, inst_pip; 

    try {
      IOrder o_lossing = null;
      double o_loss = 0;
      for (IOrder o : engine.getOrders()) {

        if(o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED)
          continue;

        cmd = o.getOrderCommand();
        if(cmd != OrderCommand.SELL && cmd != OrderCommand.BUY)
          continue;

        inst = o.getInstrument();
        open_price = o.getOpenPrice();
        sl_price = o.getStopLossPrice();
        closing = false;

        tick = getLastTick(inst);
        if(tick == null)
          continue;

        inst_pip = inst.getPipValue();
        atleast = (tick.getAsk()/(inst_pip/10))*0.00001;
        if(Double.compare(atleast, 1) < 0)
          atleast = 1/atleast;
        atleast += configs.trail_step_1st_min;
        if(Double.compare(atleast, configs.trail_step_1st_min) < 0)
          atleast = configs.trail_step_1st_min;
        if(Double.compare(o.getProfitLossInPips(), -atleast) >= 0)
          continue;

        step = (tick.getAsk() / (inst_pip/10)) * 0.00001;
        if(step < 1)
          step = 1/step;
        step *= 2.5;
        step += (tick.getBid() - tick.getAsk()) / inst_pip;
        if(Double.compare(atleast, step) > 0)
          step = atleast;
        if(Double.compare(o.getProfitLossInPips(), -step) > 0)
          continue;
              
        if(Double.isNaN(sl_price) || Double.compare(sl_price, 0) == 0) {
          closing = true;
        } else if(cmd == OrderCommand.BUY && open_price > sl_price) {
          closing = true;
        } else if (cmd == OrderCommand.SELL && open_price < sl_price) {
          closing = true;
        }
        if(closing) {
          if(o_lossing == null || o_loss > o.getProfitLossInAccountCurrency()) {
            o_loss = o.getProfitLossInAccountCurrency();
            o_lossing = o;
            SharedProps.print(
              "closeOrder: nominated-order " + inst.toString() + " " + cmd + 
              " step=" + step +
              " loss=" + o_lossing.getProfitLossInUSD() + "/" + o_loss + "/" + o.getProfitLossInPips()
            );
          }
        }
      }
      if(o_lossing == null)
        return;
      SharedProps.print(
        "closeOrder: closing-order " + o_lossing.getInstrument().toString() + " " + o_lossing.getOrderCommand() + " " + 
        " loss=" + o_lossing.getProfitLossInUSD() + "/" + o_loss + "/" + "/" + o_lossing.getProfitLossInPips()
      );
      context.executeTask(new CloseOrder(o_lossing));

    } catch (Exception e) {
      SharedProps.print("closeOrder exec() E: " + e.getMessage() + " " +
                        "Thread: " + Thread.currentThread().getName() + " " + e);
    }
  }

  private class CloseOrder implements Callable<IOrder> {
    private final IOrder o;

    public CloseOrder(IOrder order) {
      o = order;
    }
    public IOrder call() {
      try {
        double amt = SharedProps.round(o.getAmount() / configs.gain_close_amount_devider, 3);
        if(Double.compare(amt, 0.001) < 0)
          amt = 0.001;
        if(Double.compare(SharedProps.round(o.getAmount() - amt, 3), 0.001) < 0)
          amt = 0.0;
        o.close(amt);
        apply_gainbase();

      } catch (Exception e){
        SharedProps.print("closeOrder call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +o.getInstrument());
      }
      return o;
    }
  }


  //
  class ProfitLoss {
    public double profit = 0;
    public double loss = 0;
  }
  private ProfitLoss getOrdersProfit() {
    ProfitLoss profit_loss = new ProfitLoss();

    double add_profit, add_loss, inst_pip_v, open_price, sl_price, pip_acc_v;
    Instrument inst;

    try {
      for( IOrder p : engine.getOrders()) {

        if(p.getState() != IOrder.State.FILLED && p.getState() != IOrder.State.OPENED)
          continue;

        OrderCommand cmd = p.getOrderCommand();
        if (cmd != OrderCommand.SELL && cmd != OrderCommand.BUY)
          continue;

        if(Double.compare(p.getProfitLossInPips(), 0) <= 0) {
          add_loss = p.getProfitLossInAccountCurrency();
          if(Double.isNaN(add_loss)) return null;
          profit_loss.loss += add_loss;
          continue;
        }

        sl_price = p.getStopLossPrice();
        if(Double.isNaN(sl_price) || Double.compare(sl_price, 0) == 0){
          if(Double.compare(p.getProfitLossInAccountCurrency(), 0) >= 0) {
            add_profit = p.getProfitLossInAccountCurrency();
            if(Double.isNaN(add_profit)) return null;
            profit_loss.profit += add_profit;
          }
          continue;
        }

        inst = p.getInstrument();
        inst_pip_v = inst.getPipValue();
        open_price = p.getOpenPrice();
        pip_acc_v = p.getProfitLossInAccountCurrency() / p.getProfitLossInPips();
        if(Double.isNaN(pip_acc_v)) {
          profit_loss = null;
          break;
        }

        if(cmd == OrderCommand.BUY && open_price < sl_price) {
          add_profit = ((getLastTick(inst).getBid()-sl_price)/inst_pip_v)*pip_acc_v;
          if(Double.isNaN(add_profit)) return null;
          profit_loss.profit += add_profit;

        } else if (cmd == OrderCommand.SELL && open_price > sl_price){
          add_profit = ((sl_price-getLastTick(inst).getAsk())/inst_pip_v)*pip_acc_v;
          if(Double.isNaN(add_profit)) return null;
          profit_loss.profit += add_profit;
        }
      }
    } catch (Exception e) {
      SharedProps.print("getOrdersProfit E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e);
      profit_loss = null;
    }
    return profit_loss;
  }

  //
  private ITick getLastTick(Instrument inst) {
    for(;;) {
      try{
        ITick t = history.getLastTick(inst);
        if(t != null && !Double.isNaN(t.getAsk()) && !Double.isNaN(t.getBid()))
          return t;
      } catch (Exception e){}
      try{ Thread.sleep(2); } catch (Exception e){}
    }
  }
  private double getOffersDifference(Instrument inst) {
    ITick t = getLastTick(inst);
    return t.getAsk()-t.getBid();
  }

  //

  private int email_hour_c=0;
  private int email_hour=25;

  private void send_mail_status() {
    if(!configs.emailReports || 
        SharedProps.reports_q.isEmpty() ||
        configs.reportEmail.isEmpty() ) return;

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

      context.getUtils().sendMail(
        configs.reportEmail, 
        configs.reportName+" acc status", 
        content
      );
      SharedProps.print("send_mail_status content: "+content);
      email_hour_c++;

    } catch (Exception e) {
      SharedProps.print("sendMail E: "+e.getMessage());
    }
  }

  private String log_file = "";
  private FileWriter log_fd = null;
  private void commit_log() {
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
      SharedProps.print("commit_log: " + e.getMessage());
    }
  }

  @Override
  public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) {}
  @Override
  public void onTick(Instrument instrument, ITick tick) {}
  @Override
  public void onStop(){
    stop_run.set(true);
  }
}
