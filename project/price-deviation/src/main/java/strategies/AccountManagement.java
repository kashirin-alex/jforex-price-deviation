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
  public IClient          client;
  public long             strategyId;

  private IEngine         engine;
  private IHistory        history;
  private IContext        context;
  private IAccount        account;
  private JFUtils         utils;

  private int             equity_gain_change_count = 0;
  private double          email_equity_gain_change_track = 0;
  private boolean         waiting_instruments_apply_change = false;

  @Override
  public void onStart(IContext ctx) {
    SharedProps.print("onStart start: account-manager");
    configs = SharedProps.configs;

    context = ctx;
    engine = ctx.getEngine();
    history = ctx.getHistory();
    utils = ctx.getUtils();

    account = ctx.getAccount();
    configs.account_currency = account.getAccountCurrency();

    if(configs.server_time_offset == 0) {
      configs.server_time_offset = ((long)((context.getTime() - System.currentTimeMillis()) / 3600000)) * 3600000;
      SharedProps.print("acc server_time_offset:" + configs.server_time_offset);
    }

    new Thread(new Runnable() {
      @Override
      public void run() { bg_tasks(); }
    }).start();
  }

  public void adjust_free_margin() {
    try {
      double eq = account.getEquity();
      if(!Double.isNaN(eq)) {
        double used_margin = account.getUsedMargin();
        if(!Double.isNaN(used_margin)) {
          SharedProps.set_free_margin(eq - used_margin);
        }
      }
    } catch(Exception e) { }
  }

  @Override
  public void onAccount(IAccount acc) {
    adjust_free_margin();
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
        case ORDER_SUBMIT_OK:
        case ORDER_SUBMIT_REJECTED:
        case ORDER_CHANGED_OK:
        case ORDER_CHANGED_REJECTED:
        case ORDERS_MERGE_OK:
        case ORDERS_MERGE_REJECTED:
        case ORDER_FILL_OK:
        case ORDER_FILL_REJECTED:
        case ORDER_CLOSE_OK:
        case ORDER_CLOSE_REJECTED:
        case STOP_LOSS_LEVEL_CHANGED:
          break;

        default:
          if(configs.debug.get())
            SharedProps.print("acc " + message.toString());
          break;
      }
    } catch (Exception e) {
      SharedProps.print("acc onMessage E: " + e + " Thread: " + Thread.currentThread().getName());
    }
  }

  @Override
  public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) { }
  @Override
  public void onTick(Instrument instrument, ITick tick) {
    adjust_free_margin();
  }
  @Override
  public void onStop() {
    stop_run.set(true);
    if(configs.is_email_report_enabled())
      SharedProps.reports_q.add("Account-Manager stop:" + strategyId);
    SharedProps.print("stop: account-manager");
  }

  //
  private void bg_tasks() {
    long ts = SharedProps.get_sys_ts();
    long ten_secs = ts;
    long one_minute = ts;
    long five_minutes = ts;
    long wait = 0;

    while(!stop_run.get()) {
      try {
        wait = 5000;
        pid_restart.set(false);
        ts = SharedProps.get_sys_ts();

        if(ts >= one_minute) {
          configs.getConfigs();
          double gainbase = configs.equity_gain_base.get();
          if(Double.isNaN(gainbase) || Double.compare(gainbase, 0.0) == 0)
            update_equity_gain_base(gainbase, Double.NaN);
          one_minute = ts + 60000;
        }

        if(ts >= five_minutes) {
          set_instruments();
          release_memory();
          five_minutes = ts + 300000;
        }

        if(waiting_instruments_apply_change) {
          equity_change();
          wait = 500;

        } else if(ts >= ten_secs) {
          SharedProps.print("bg_tasks");
          equity_change();
          ten_secs = ts + 10000;
        }

        send_mail_status();

        Thread.sleep(wait);

      } catch (Exception e) {
        SharedProps.print("acc bg_tasks E: " + e + " Thread: " + Thread.currentThread().getName());
      }
    }
  }


  private void set_instruments() {
    try {
      if(configs.account_currency.getCurrencyCode().compareTo("ILS") == 0)
        context.setSubscribedInstruments(java.util.Collections.singleton(Instrument.USDILS), true);
      for(Instrument inst : StrategyConfigs.instruments) {
        context.setSubscribedInstruments(java.util.Collections.singleton(inst), true);
      }
    } catch (Exception e) {
      SharedProps.print("acc set_instruments E: " + e + " Thread: " + Thread.currentThread().getName());
    }
  }

  private void release_memory() {
    try {
      for (String k : SharedProps.oBusy.keySet()) {
        if(SharedProps.get_sys_ts() - SharedProps.oBusy.get(k) > 60*60*1000)
          SharedProps.oBusy.remove(k);
      }
    } catch (Exception e) {
      SharedProps.print("acc release_memory E: " + e + " Thread: " + Thread.currentThread().getName());
    }
  }

  private void update_equity_gain_base(double gainbase, double eq) {
    try {
      Boolean need = Double.isNaN(eq);

      File file = new File(SharedProps.strategy_dir + "status.dat");
      if(need) {
        if(file.exists()) {
          FileReader fr = new FileReader(file);
          BufferedReader reader  = new BufferedReader(fr);
          String line = reader.readLine();
          fr.close();
          if(line != null)
            eq = Double.valueOf(line);
        }

        need = Double.isNaN(eq);
        if(need) {
          eq = account.getEquity();
          if(Double.isNaN(eq))
            return;
          eq = SharedProps.round(eq, 0);
        }

        if(Double.isNaN(gainbase) || Double.compare(gainbase, 0.0) == 0) {
          configs.set_equity_gain_base(eq);
          gainbase = eq;
        }
      }

      if(need || Double.compare(eq, gainbase) > 0) {
        SharedProps.print("Update Equity Gain Base: " + (need ? 0 : gainbase) + " to: " + eq);

        FileWriter writer = new FileWriter(file);
        writer.write(Double.toString(SharedProps.round(gainbase, 0)));
        writer.flush();
        writer.close();
      }
    } catch (Exception e) {
      SharedProps.print("acc update_equity_gain_base E: " + e + " Thread: " + Thread.currentThread().getName());
    }
  }


  //
  private void equity_change() {
    try {
      double eq = account.getEquity();
      if(Double.isNaN(eq))
        return;
      double gainbase = configs.equity_gain_base.get();
      if(Double.isNaN(gainbase) || Double.compare(gainbase, 0) <= 0)
        return;

      if(configs.is_email_report_enabled()) {
        double rate = configs.email_equity_gain_change.get();
        double change = eq - email_equity_gain_change_track;
        if(Double.compare(rate, 0.0) > 0 &&
           Double.compare(Math.abs(change), email_equity_gain_change_track * rate) >= 0) {
          if(Double.compare(email_equity_gain_change_track, 0.0) > 0) {
            SharedProps.reports_q.add(
              "Equity changed " + SharedProps.round(change, 2) + account.getAccountCurrency() +
              " (" + SharedProps.round(email_equity_gain_change_track, 2) + " >> " + SharedProps.round(eq, 2) + ")" +
              " free-margin=" + SharedProps.round(SharedProps.get_free_margin(), 2) +
              " base=" + SharedProps.round(gainbase, 2)
            );
          }
          email_equity_gain_change_track = eq;
        }
      }

      double unsettled = get_unsettled_profit();
      if(Double.isNaN(unsettled))
        return;
      if(Double.compare((eq - unsettled) - gainbase, gainbase * configs.equity_gain_change.get()) >= 0) {
        equity_gain_change_count += 1;
        SharedProps.print("equity_change eq=" + eq + " unsettled=" + unsettled + " gainbase=" + gainbase + " check=" + equity_gain_change_count);
        if(equity_gain_change_count >= configs.equity_gain_change_count.get() &&
           instruments_check_applied_gainbase()) {
          SharedProps.print("equity_change eq=" + eq + " unsettled=" + unsettled + " gainbase=" + gainbase + " applied");
          eq = SharedProps.round(eq - unsettled, 0);
          configs.set_equity_gain_base(eq);
          update_equity_gain_base(gainbase, eq);
          equity_gain_change_count = 0;
        }
      } else if(equity_gain_change_count > 0) {
        SharedProps.print("equity_change eq=" + eq + " unsettled=" + unsettled + " gainbase=" + gainbase + " reset");
        instruments_apply_gainbase(false);
        equity_gain_change_count = 0;
      }

    } catch (Exception e) {
      SharedProps.print("acc equity_change E: " + e + " Thread: " + Thread.currentThread().getName());
    }
  }

  private double get_unsettled_profit() throws Exception {
    double unsettled = 0.0;
    for(Instrument inst : StrategyConfigs.instruments) {
      if(!SharedProps.inst_active.get(inst.toString()))
        continue;
      
      List<IOrder> orders = engine.getOrders(inst);
      if(orders.size() == 0)
        continue;
      
      ITick tick = history.getLastTick(inst);
      if(tick == null)
        return Double.NaN;
      double lastTickAsk = tick.getAsk();
      if(Double.isNaN(lastTickAsk))
        return Double.NaN;
      double lastTickBid = tick.getBid();
      if(Double.isNaN(lastTickBid))
        return Double.NaN;
      
      double inst_amt_min = configs.get_inst_amt_min(inst);
      inst_amt_min = SharedProps.round(inst_amt_min, configs.get_double_scale(inst_amt_min));
      double inst_amt_ratio = (int)SharedProps.round(1/inst_amt_min, 0);
      double diff_pip = (lastTickAsk - lastTickBid) / inst.getPipValue();

      for(IOrder o : orders) {
        OrderCommand cmd = o.getOrderCommand();
        if(o.getState() != IOrder.State.FILLED && cmd != OrderCommand.BUY && cmd != OrderCommand.SELL)
          continue;

        double pl = Math.abs(o.getProfitLossInAccountCurrency());
        double sl = o.getStopLossPrice();
        if(Double.compare(sl, 0.0) == 0) {
          double pip = Math.abs(o.getProfitLossInPips());
          double rate = Double.NaN;
          if(Double.compare(pl, 1.0) > 0 && Double.compare(pip, 1.0) > 0) {
            rate = (pl/pip) / (o.getAmount() * inst_amt_ratio);
          } else {
            try {
              rate = utils.convertPipToCurrency(
                inst,
                configs.account_currency,
                (o.getOrderCommand() == OrderCommand.BUY ? OfferSide.ASK : OfferSide.BID)
              );
            } catch (Exception e) { }
            if(Double.isNaN(rate) || Double.compare(rate, 0.0) == 0)
              return Double.NaN;
            rate *= (1000); // account_currency == pip-value (0.0001)
          }
          unsettled += 1.5 * rate * diff_pip;
          
        } else {
          double price = o.getOpenPrice();
          if(o.getOrderCommand() == OrderCommand.BUY) {
            unsettled += pl * (1.0 - ((sl - price) / (lastTickAsk - price)));
          } else if(o.getOrderCommand() == OrderCommand.SELL) {
            unsettled += pl * (1.0 - ((price - sl) / (price - lastTickBid)));
          }
        }
      }
      
    }
    return unsettled;
  }

  private void instruments_apply_gainbase(boolean apply) {
    if(apply ? !waiting_instruments_apply_change : waiting_instruments_apply_change) {
      for(String inst_str : SharedProps.inst_gain_change_state.keySet()) {
        SharedProps.inst_gain_change_state.get(inst_str).set(apply);
      }
      waiting_instruments_apply_change = apply;
      SharedProps.print("instruments_apply_gainbase apply=" + apply);
    }
  }

  private boolean instruments_check_applied_gainbase() {
    instruments_apply_gainbase(true);

    SharedProps.print("instruments_check_applied_gainbase");
    for(String inst_str : SharedProps.inst_gain_change_state.keySet()) {
      if(SharedProps.inst_gain_change_state.get(inst_str).get())
        return false;
    }
    waiting_instruments_apply_change = false;
    return true;
  }

  //
  private int email_hour_c = 0;
  private int email_hour = 25;

  private void send_mail_status() {
    if(SharedProps.reports_q.isEmpty())
      return;
    if(!configs.is_email_report_enabled()) {
      do SharedProps.print("email-msg skip:" + SharedProps.reports_q.poll());
      while(!SharedProps.reports_q.isEmpty());
      return;
    }

    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    if(email_hour_c == 100) {
      if(email_hour == cal.get(Calendar.HOUR_OF_DAY))
        return;
      email_hour_c = 0;
      email_hour = cal.get(Calendar.HOUR_OF_DAY);
    }
    try{
      String content = "";
      while(!SharedProps.reports_q.isEmpty()) {
        if(content.length() + SharedProps.reports_q.peek().length() > 500)
          break;
        content += SharedProps.reports_q.poll();
        try{ Thread.sleep(300); } catch (Exception e) { }
      }

      final String email = configs.reportEmail.get();
      final String report = configs.reportName.get();
      utils.sendMail(email, report, content);
      ++email_hour_c;

      if(configs.debug.get()) {
        SharedProps.print(
          "send_mail_status count=" + email_hour_c +
          " report=" + report +
          " email=" + email +
          " content: \n" + content
        );
      }
    } catch (Exception e) {
      SharedProps.print("acc send_mail_status E: " + e + " Thread: " + Thread.currentThread().getName());
    }
  }

}
