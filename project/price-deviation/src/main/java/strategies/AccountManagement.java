// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.*;
import com.dukascopy.api.system.IClient;
import com.dukascopy.api.IEngine.OrderCommand;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.Callable;

public class AccountManagement implements IStrategy{

  public boolean stop_run = false;
  public boolean pid_restart = false;

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
    while(!stop_run){
      commit_log();
      try{
        pid_restart=false;
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
        }
        //if(SharedProps.get_sys_ts() - one_hour_timer > 3600000) {
         // setInstrumentsAmountRatio();
        //}
        if(SharedProps.get_sys_ts() - ten_secs_timer > 10000) {
          SharedProps.print("bg_tasks");
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
      if(configs.account_currency.getCurrencyCode().compareTo("ILS") == 0)
        context.setSubscribedInstruments(java.util.Collections.singleton(Instrument.USDILS), true);
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

      if(Double.compare(eq, gainbase * configs.gain_close_overloss_atleast_percent) >= 0 && 
         Double.compare(eq - gainbase, Math.abs(pl.loss) * configs.gain_close_overloss_percent) >= 0) {
        SharedProps.print("closeOrdersOnProfit overloss eq=" + eq + " gainbase=" + gainbase +
            " " + (eq - gainbase) + " >= " + (Math.abs(pl.loss) * configs.gain_close_overloss_percent) +
            " c=" + (gain_close_count + 1));
        gain_close_count += 1;
        if(gain_close_count == configs.gain_close_count) {
          gain_close_count = 0;
          closeOrders("overloss");
        }
      } else {
        gain_close_count = 0;
      }

    } catch (Exception e) {
      SharedProps.print("closeOrdersOnProfit E: "+e.getMessage()+" "+
                        "Thread: "+Thread.currentThread().getName()+" " + e);
    }
  }

  private void closeOrders(String close_type) {

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

        if(o.getLabel().contains("Signal:") && !configs.manageSignals)
          continue;

        if(o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED)
          continue;

        cmd = o.getOrderCommand();
        if(cmd != OrderCommand.SELL && cmd != OrderCommand.BUY)
          continue;

        inst = o.getInstrument();
        open_price = o.getOpenPrice();
        sl_price = o.getStopLossPrice();
        closing = false;

        switch(close_type) {
          case "overloss":
            tick = getLastTick(inst);
            if(tick == null)
              continue;

            inst_pip = inst.getPipValue();
            atleast = (tick.getAsk()/(inst_pip/10))*0.00001;
            if(Double.compare(atleast, 1) < 0)
              atleast = 1/atleast;
            atleast *= configs.tp_step_1st_min;//
            if(Double.compare(atleast, configs.tp_step_1st_min) < 0)
              atleast = configs.tp_step_1st_min;
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
              if(o_lossing == null || o_loss > o.getProfitLossInPips()) {
                o_loss = o.getProfitLossInPips();
                o_lossing = o;
                SharedProps.print(
                  "CloseOrders "+close_type+": nominated-order " + inst.toString() + " " + cmd + 
                  " step=" + step + " loss=" + o_lossing.getProfitLossInAccountCurrency() + "/" + o_loss
                );
              }
            }
            break;
        }
      }
      if(o_lossing == null)
        return;
      SharedProps.print(
        "CloseOrders "+close_type+": closing-order " + o_lossing.getInstrument().toString() + " " + o_lossing.getOrderCommand() + " " + 
        " loss=" + o_lossing.getProfitLossInAccountCurrency() + "/" + o_lossing.getProfitLossInPips()
      );
      context.executeTask(new CloseOrder(o_lossing));

    } catch (Exception e) {
      SharedProps.print("CloseOrders call() E: " + e.getMessage() + " " +
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
        o.close();

        double eq_unsettled = account.getEquity();
        ProfitLoss pl = getOrdersProfit();
        if(!Double.isNaN(eq_unsettled) && pl != null) {
          double eq = eq_unsettled - pl.profit - configs.amount_bonus;
          if (Double.compare(eq, configs.get_gainBase()) > 0)
            configs.set_gainBase(eq);
        }
      }catch (Exception e){
        SharedProps.print("OrderSetSL call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +o.getInstrument());
      }
      return o;
    }
  }


  /*
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
  */

  /*
  private void closeOrders(String close_type) {
    gain_close_count += 1;
    if(gain_close_count < configs.gain_close_count)
      return;
    gain_close_count = 0;


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

          if(o.getLabel().contains("Signal:") && !configs.manageSignals)
            continue;

          if(o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED)
            continue;

          cmd = o.getOrderCommand();
          if(cmd != OrderCommand.SELL && cmd != OrderCommand.BUY)
            continue;

          inst = o.getInstrument();
          open_price = o.getOpenPrice();
          sl_price = o.getStopLossPrice();
          closing = false;

          switch(close_type) {
            case "overloss":
              tick = getLastTick(inst);
              if(tick == null)
                continue;

              inst_pip = inst.getPipValue();
              atleast = (tick.getAsk()/(inst_pip/10))*0.00001;
              if(Double.compare(atleast, 1) < 0)
                atleast = 1/atleast;
              atleast *= configs.tp_step_1st_min;//
              if(Double.compare(atleast, configs.tp_step_1st_min) < 0)
                atleast = configs.tp_step_1st_min;
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
                if(o_lossing == null || o_loss > o.getProfitLossInPips()) {
                  o_loss = o.getProfitLossInPips();
                  o_lossing = o;
                  SharedProps.print(
                    "CloseOrders "+close_type+": nominated-order " + inst.toString() + " " + cmd + 
                    " step=" + step + " loss=" + o_lossing.getProfitLossInAccountCurrency() + "/" + o_loss
                  );
                }
              }
              break;
          }
        }
        if(o_lossing == null)
          return this;
        SharedProps.print(
          "CloseOrders "+close_type+": closing-order " + o_lossing.getInstrument().toString() + " " + o_lossing.getOrderCommand() + " " + 
          " loss=" + o_lossing.getProfitLossInAccountCurrency() + "/" + o_lossing.getProfitLossInPips()
        );
        o_lossing.close();

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
  */

  /*
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
  */



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
    stop_run = true;
  }
}
