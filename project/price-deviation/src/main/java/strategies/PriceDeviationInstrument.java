// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.*;
import com.dukascopy.api.IEngine.OrderCommand;
import com.dukascopy.api.IIndicators.AppliedPrice;
import com.dukascopy.api.system.IClient;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;


public class PriceDeviationInstrument implements IStrategy {

  public AtomicBoolean stop_run       = new AtomicBoolean(false);
  public AtomicBoolean pid_restart    = new AtomicBoolean(false);
  public AtomicBoolean state_gain_chg = new AtomicBoolean(false);

  public IClient          client;
  public long             strategyId;
  public Instrument       inst;
  public Thread           inst_thread;

  private IEngine         engine;
  private IHistory        history;
  private IContext        context;
  private IDataService    dataService;
  private JFUtils         utils;
  private StrategyConfigs configs;

  private String          inst_str;
  private double          inst_pip;
  private int             inst_scale;
  private double          inst_amt_min;
  private int             inst_amt_scale;
  private int             inst_amt_ratio;

  private ReentrantLock   lock = new ReentrantLock();
  private Condition       work = lock.newCondition();

  private AtomicLong[]    inst_busy_exec_ts = {
    new AtomicLong(SharedProps.get_sys_ts() + 1000),
    new AtomicLong(SharedProps.get_sys_ts() + 1000),
    new AtomicLong(SharedProps.get_sys_ts() + 1000)
  };
  private long            inst_exec_ts = 0;
  private double          inst_offer_diff = 0.0;
  private long            inst_offer_diff_ts = 0;

  private ConcurrentHashMap<String, Double> orders_trailingstep = new ConcurrentHashMap<>();

  private long log_info_ts = 0;

  @Override
  public void onAccount(IAccount account) { }

  @Override
  public void onStart(IContext ctx) {

    configs = SharedProps.configs;
    context = ctx;
    engine = context.getEngine();
    history = context.getHistory();
    dataService = ctx.getDataService();
    utils = ctx.getUtils();

    context.setSubscribedInstruments(java.util.Collections.singleton(inst), true);

    inst_str = inst.toString();
    inst_pip = inst.getPipValue();
    inst_scale = inst.getPipScale();

    inst_amt_min = configs.get_inst_amt_min(inst);
    inst_amt_scale = configs.get_double_scale(inst_amt_min);
    inst_amt_min = SharedProps.round(inst_amt_min, inst_amt_scale);
    inst_amt_ratio = (int)SharedProps.round(1/inst_amt_min, 0);

    SharedProps.print(
      inst_str +
      " start:" +
      " pip=" + inst_pip +
      " scale=" + inst_scale +
      " amt_min=" + inst_amt_min +
      " amt_scale=" + inst_amt_scale +
      " amt_ratio=" + inst_amt_ratio +
      " MinTradeAmount=" + inst.getMinTradeAmount() +
      " TradeAmountIncrement=" + inst.getTradeAmountIncrement() +
      " AmountPerContract=" + inst.getAmountPerContract()
    );

    SharedProps.inst_gain_change_state.put(inst_str, state_gain_chg);

    new Thread(new Runnable() {
      @Override
      public void run() { runner(); }
    }).start();
  }

  @Override
  public void onTick(final Instrument instrument, ITick tick) {
    if(instrument.toString().equals(inst_str)) {
      notify_work();
    }
  }

  @Override
  public void onMessage(IMessage message) {
    try {
      IOrder order = message.getOrder();
      if(order == null)
        return;
      Instrument o_inst = order.getInstrument();
      if(!o_inst.toString().equals(inst_str))
        return;
      long ts = SharedProps.get_sys_ts();

      switch(message.getType()) {
        case CALENDAR:
        case CONNECTION_STATUS:
        case MAIL:
        case NEWS:
        case WITHDRAWAL:
        case NOTIFICATION:
        case STRATEGY_BROADCAST:
        case INSTRUMENT_STATUS:
          break;

        case ORDER_SUBMIT_OK:
          if(configs.debug.get())
            SharedProps.print(inst_str + " submit " + order.getOrderCommand() + " ok " + order);
          set_busy_exec_ts(order.getOrderCommand(), ts + 5000);
          break;
        case ORDER_SUBMIT_REJECTED:
          if(configs.debug.get())
            SharedProps.print(inst_str + " submit " + order.getOrderCommand() + " rejected: " + message);
          set_busy_exec_ts(order.getOrderCommand(), ts + 10000);
          SharedProps.oBusy.remove(order.getId());
          break;

        case ORDER_CHANGED_OK:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Order changed ok: " + order);
          SharedProps.oBusy.put(order.getId(), ts + 900);
          break;
        case ORDER_CHANGED_REJECTED:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Order change rejected: " + message);
          SharedProps.oBusy.put(order.getId(), ts - 1000);
          break;

        case ORDERS_MERGE_OK:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Orders merged ok: " + order);
          orders_trailingstep.remove(order.getId());
          inst_busy_exec_ts[2].set(ts + 500);
          break;
        case ORDERS_MERGE_REJECTED:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Orders merge rejected: " + message);
          SharedProps.oBusy.remove(order.getId());
          inst_busy_exec_ts[2].set(0);
          break;

        case ORDER_FILL_OK:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Order fill ok: " + order);
          set_busy_exec_ts(order.getOrderCommand(), ts + 500);
          break;
        case ORDER_FILL_REJECTED:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Order fill rejected: " + message);
          set_busy_exec_ts(order.getOrderCommand(), ts + 1000);
          SharedProps.oBusy.put(order.getId(), ts - 1000);
          break;

        case ORDER_CLOSE_OK:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Order close ok: " + order);
          orders_trailingstep.remove(order.getId());
          break;
        case ORDER_CLOSE_REJECTED:
          if(configs.debug.get())
            SharedProps.print(inst_str + " Order close rejected: " + message);
          SharedProps.oBusy.put(order.getId(), ts - 1000);
          break;

        case STOP_LOSS_LEVEL_CHANGED:
          if(configs.debug.get())
            SharedProps.print(inst_str + " ST changed ok: " + order);
          break;

        default:
          if(configs.debug.get())
            SharedProps.print(inst_str + " default msg-event: " + order);
          break;

      }

      notify_work();

    } catch (Exception e) {
    }
  }

  @Override
  public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) {}

  @Override
  public void onStop() {
    stop_run.set(true);
    notify_work();

    if(configs.is_email_report_enabled())
      SharedProps.reports_q.add(inst_str + " stop:" + strategyId);
    SharedProps.print(inst_str + " stop");
  }



  private void runner() {
    while(!stop_run.get()) {
      try {
        boolean inst_active = SharedProps.inst_active.get(inst_str);
        lock.lock();
        work.await(
          inst_active ? configs.execute_inst_ms.get() : 10000,
          TimeUnit.MILLISECONDS
        );
        lock.unlock();
        if(!inst_active) {
          pid_restart.set(false);
          if(state_gain_chg.get() && engine.getOrders(inst).size() == 0)
            state_gain_chg.set(false);
          continue;
        }

        ITick tick = history.getLastTick(inst);
        if(tick == null)
          continue;
        double lastTickAsk = tick.getAsk();
        if(Double.isNaN(lastTickAsk))
          continue;
        double lastTickBid = tick.getBid();
        if(Double.isNaN(lastTickBid))
          continue;

        manage(tick.getTime(), lastTickAsk, lastTickBid);

      } catch (Exception e) {
        SharedProps.print(inst_str + " runner E: " + e + " Thread: " + Thread.currentThread().getName());
      }

      pid_restart.set(false);
    }
  }

  private void notify_work() {
    lock.lock();
    work.signal();
    lock.unlock();
  }



  //
  private void manage(final long lastTickTs, final double lastTickAsk, final double lastTickBid) throws Exception {
    final long ts = SharedProps.get_sys_ts();

    if(inst_busy_exec_ts[2].get() > ts)
      return;

    final boolean debug = configs.debug.get();

    List<IOrder> orders = new ArrayList<>();
    OrderCommand cmd;
    double o_profit, o_cost, o_sl;

    double amt_buy  = 0.0;
    double amt_sell = 0.0;
    int orders_buy  = 0;
    int orders_sell = 0;

    for(IOrder o : engine.getOrders(inst)) {
      cmd = o.getOrderCommand();
      if(cmd == OrderCommand.BUY || cmd == OrderCommand.PLACE_BID || cmd == OrderCommand.BUYLIMIT || cmd == OrderCommand.BUYSTOP) {
        amt_buy += o.getAmount();
        ++orders_buy;
      } else if(cmd == OrderCommand.SELL || cmd == OrderCommand.PLACE_OFFER || cmd == OrderCommand.SELLLIMIT || cmd == OrderCommand.SELLSTOP) {
        amt_sell += o.getAmount();
        ++orders_sell;
      }
      if(o.getState() == IOrder.State.FILLED && (cmd == OrderCommand.BUY || cmd == OrderCommand.SELL)) {
        orders.add(o);
        if(!SharedProps.oBusy.containsKey(o.getId()))
          SharedProps.oBusy.put(o.getId(), ts);
      }
    }
    amt_buy = SharedProps.round(amt_buy, inst_amt_scale + 3);
    amt_sell = SharedProps.round(amt_sell, inst_amt_scale + 3);
    final boolean inst_starts = Double.compare(amt_buy, 0.0) == 0 && Double.compare(amt_sell, 0.0) == 0;
    final boolean inst_flat = Double.compare(amt_buy, amt_sell) == 0;
    final boolean log_at_ts = debug && log_info_ts < ts;
    if(log_at_ts)
      log_info_ts = ts + 5000;

    if(inst_starts) {
      if(state_gain_chg.get()) {
        state_gain_chg.set(false);
        return;
      }
      long day_secs = (ts/1000) - ((long)(ts/86400000))*86400;
      long check_ts = configs.execute_inst_skip_day_end_secs.get() - (86400 - day_secs);
      if(check_ts >= 0) {
        if(log_at_ts)
          SharedProps.print(inst_str + " skip instrument init-execution end-of-day for=" + (check_ts - configs.execute_inst_skip_day_end_secs.get()) + "s");
        return;
      }
      check_ts = configs.execute_inst_skip_day_begin_secs.get() - day_secs;
      if(check_ts >= 0) {
        if(log_at_ts)
          SharedProps.print(inst_str + " skip instrument init-execution begin-of-day for=" + check_ts + "s");
        return;
      }
      if(dataService.isOfflineTime(ts + configs.execute_inst_skip_weekend_secs.get() * 1000, inst)) {
        if(log_at_ts)
          SharedProps.print(inst_str + " skip instrument init-execution before offline-time");
        return;
      }
    }



    final boolean trail_managed = configs.trail_managed.get();
    final boolean trail_at_one_side = configs.trail_at_one_side.get();
    final double trail_step_1st_min = configs.trail_step_1st_min.get();
    final double open_followup_amount_muliplier = configs.open_followup_amount_muliplier.get();
    final double trail_step_entry_multiplier = configs.trail_step_entry_multiplier.get();
    final double trail_step_rest_plus_gain = configs.trail_step_rest_plus_gain.get();

    final double amt_one = SharedProps.round(configs.amount.get() * inst_amt_min, inst_amt_scale + 3);

    double _step = (lastTickAsk / (inst_pip/10)) * 0.00001;
    if(Double.compare(_step, 1.0) < 0)
     _step = 1/_step;
    _step *= trail_step_1st_min;
    if(Double.compare(_step, trail_step_1st_min) < 0)
      _step = trail_step_1st_min;
    _step /= configs.trail_step_1st_divider.get();
    if(Double.compare(_step, trail_step_1st_min) < 0)
      _step = trail_step_1st_min;

    final double step_1st_pip = SharedProps.round(_step, 1);
    final double step_1st = SharedProps.round(step_1st_pip * inst_pip, inst_scale + 1);
    final double offer_diff = get_offer_diff(lastTickTs, lastTickAsk - lastTickBid) / inst_pip;

    List<IOrder> mergeBuyOrders = new ArrayList<>();
    List<IOrder> mergeSellOrders = new ArrayList<>();


    if(log_at_ts) {
      SharedProps.print(
        inst_str +
        " amt=" + amt_one +
        " step=" + step_1st_pip + "/" + step_1st +
        " diff=" + SharedProps.round(offer_diff, 1) + "/" + SharedProps.round(inst_offer_diff/inst_pip, 1) +
        ", orders=" + orders.size() +
        " buy=" + amt_buy + "/" + orders_buy +
        " sell=" + amt_sell + "/" + orders_sell
      );
    }

    if(trail_managed && orders.size() > 0) {
      for(IOrder o : orders) {
        o_sl = orders_trailingstep.getOrDefault(o.getId(), 0.0);
        if (Double.compare(o_sl, 0.0) == 0 || ts - SharedProps.oBusy.get(o.getId()) < 888)
          continue;
        double diff = 0.0;
        double price = 0.0;
        if(o.getOrderCommand() == OrderCommand.BUY && Double.compare(amt_buy, amt_sell) > 0) {
          price = lastTickBid;
          diff = o_sl - price;
        } else if(o.getOrderCommand() == OrderCommand.SELL && Double.compare(amt_sell, amt_buy) > 0) {
          price = lastTickAsk;
          diff = price - o_sl;
        }
        if(Double.compare(diff, 0.0) >= 0) {
          if(Double.compare(diff, step_1st) < 0) {
            SharedProps.print(
              inst_str + " Closing SL at price=" + price + " sl=" + o_sl +
              " diff=" + SharedProps.round(diff/inst_pip, 2) +
              o
            );
            executeClose(o, 0.0, price, ts);
          } else {
            orders_trailingstep.remove(o.getId());
          }
        }
      }
    }



    //// Mergeable
    boolean reach_mergable = false;

    if(orders.size() > 1) {
      for(IOrder o : orders) {
        if(o.getOrderCommand() == OrderCommand.BUY) {
          mergeBuyOrders.add(o);
        } else if(o.getOrderCommand() == OrderCommand.SELL) {
          mergeSellOrders.add(o);
        }
      }
      if(mergeSellOrders.size() < 2) {
        mergeSellOrders.clear();
      } else {
        if(debug)
          SharedProps.print(inst_str + " Merge sell");
        orders.clear();
        reach_mergable = true;
      }
      if(mergeBuyOrders.size() < 2) {
        mergeBuyOrders.clear();
      } else {
        if(debug)
          SharedProps.print(inst_str + " Merge buy");
        orders.clear();
        reach_mergable = true;
      }
    }

    if(orders.size() > 1) {
      IOrder pos = null;
      IOrder neg = null;
      for(IOrder o : orders) {
        if(pos == null || Double.compare(pos.getProfitLossInPips(), o.getProfitLossInPips()) < 0) {
          pos = o;
        }
        if(neg == null || Double.compare(neg.getProfitLossInPips(), o.getProfitLossInPips()) > 0) {
          neg = o;
        }
      }
      if(pos.getOrderCommand() != neg.getOrderCommand() &&
         Double.compare(pos.getProfitLossInAccountCurrency(), Math.abs(neg.getProfitLossInAccountCurrency())) > 0 &&
         Double.compare(pos.getAmount(), neg.getAmount()) > 0 &&
         ts - SharedProps.oBusy.get(neg.getId()) > 888 &&
         ts - SharedProps.oBusy.get(pos.getId()) > 888 ) {
        o_sl = getStopLoss(pos, trail_managed);
        double pos_cost = getCommisionPip(pos, ts);
        double neg_cost = getCommisionPip(neg, ts);
        double pos_amt = pos.getAmount() * inst_amt_ratio;
        double neg_amt = neg.getAmount() * inst_amt_ratio;
        double require = (pos.getAmount()/inst_amt_min) * configs.merge_close_neg_side_growth_rate.get();
        require += configs.merge_close_neg_side_multiplier.get();
        if(Double.compare(o_sl, 0.0) > 0) {
          double pos_dif, neg_dif;
          double price;
          if(pos.getOrderCommand() == OrderCommand.BUY) {
            pos_dif = o_sl - pos.getOpenPrice();
            price = lastTickBid;
            neg_dif = price - neg.getOpenPrice();
          } else {
            pos_dif = pos.getOpenPrice() - o_sl;
            price = lastTickAsk;
            neg_dif = neg.getOpenPrice() - price;
          }
          pos_dif *= pos_amt;
          neg_dif *= neg_amt;
          pos_dif -= pos_cost * inst_pip;
          neg_dif -= neg_cost * inst_pip;
          pos_dif -= Math.abs((pos_cost - neg_cost) * inst_pip);

          require *= step_1st;
          double reach = (pos_dif - neg_dif) / (pos_amt - neg_amt);
          boolean merge = Double.compare(reach, require) > 0;
          if(merge || debug) {
            SharedProps.print(
              inst_str + " Merge " + (merge ? "" : "nominated ") + "pos-close" +
              " pos_dif=" + SharedProps.round(pos_dif, inst_scale + 2) +
              " neg_dif=" + SharedProps.round(neg_dif, inst_scale + 2) +
              " reach=" + SharedProps.round(reach, inst_scale + 2) +
              " require=" + SharedProps.round(require, inst_scale + 2) +
              " @ " + pos.getProfitLossInAccountCurrency() + " > " + neg.getProfitLossInAccountCurrency()
            );
          }
          if(merge) {
            executeClose(neg, 0.0, price, ts);
            orders.clear();
            reach_mergable = true;
          }
        } else {
          require *= step_1st_pip;
          double pos_dif = pos.getProfitLossInPips() * pos_amt;
          double neg_dif = Math.abs(neg.getProfitLossInPips()) * neg_amt;
          double reach = (pos_dif - neg_dif) / (pos_amt - neg_amt);
          reach_mergable = Double.compare(reach, require) >= 0;

          pos_dif -= pos_cost;
          neg_dif += neg_cost;
          pos_dif -= Math.abs(pos_cost - neg_cost);
          reach = (pos_dif - neg_dif) / (pos_amt - neg_amt);
          boolean merge = Double.compare(reach, require) > 0;
          if(merge || debug) {
            SharedProps.print(
              inst_str + " Merge " + (merge ? "" : "nominated ") + "on-side" +
              " amt=" + SharedProps.round((pos_amt - neg_amt)/inst_amt_ratio, inst_amt_scale + 3) +
              " pos=" + SharedProps.round(pos_dif, 2) +
              " neg=" + SharedProps.round(neg_dif, 2) +
              " reach=" + SharedProps.round(reach, 2) +
              " require=" + SharedProps.round(require, 2) +
              " @ " + pos.getProfitLossInAccountCurrency() + " > " + neg.getProfitLossInAccountCurrency()
            );
          }
          if(merge) {
            mergeBuyOrders.add(neg);
            mergeBuyOrders.add(pos);
            orders.clear();
            reach_mergable = true;
            SharedProps.oBusy.put(neg.getId(), ts + 500);
            SharedProps.oBusy.put(pos.getId(), ts + 500);
          }
        }
      }
    }

    boolean merge = true;
    if(mergeBuyOrders.size() >= 2) {
      for(IOrder o: mergeBuyOrders) {
        if(ts + 500 < SharedProps.oBusy.get(o.getId())) {
          merge = false;
          break;
        }
        if(Double.compare(o.getStopLossPrice(), 0.0) > 0){
          executeOrderSetSL(o, 0, ts);
          merge = false;
        }
      }
      if(merge) {
        executeMergeOrders(mergeBuyOrders, ts);
      }
    }

    merge = true;
    if(mergeSellOrders.size() >= 2) {
      for(IOrder o: mergeSellOrders) {
        if(ts + 500 < SharedProps.oBusy.get(o.getId())) {
          merge = false;
          break;
        }
        if(Double.compare(o.getStopLossPrice(), 0.0) > 0){
          executeOrderSetSL(o, 0, ts);
          merge = false;
        }
      }
      if(merge) {
        executeMergeOrders(mergeSellOrders, ts);
      }
    }


    //// set StopLoss
    for(IOrder o : orders) {
      o_profit = o.getProfitLossInPips();
      if(Double.compare(o_profit, step_1st_pip * trail_step_entry_multiplier) <= 0)
        continue;

      cmd = o.getOrderCommand();
      if(trail_at_one_side
          ? (Double.compare(amt_buy, 0.0) > 0 && Double.compare(amt_sell, 0.0) > 0)
          : (inst_flat || (cmd == OrderCommand.BUY
            ? (Double.compare(amt_sell, 0.0) > 0 &&
               Double.compare(amt_buy, SharedProps.round(amt_sell + amt_one * open_followup_amount_muliplier, inst_amt_scale)) <= 0)
            : (Double.compare(amt_buy,  0.0) > 0 &&
               Double.compare(amt_sell, SharedProps.round(amt_buy + amt_one * open_followup_amount_muliplier, inst_amt_scale)) <= 0) ) ) )
        continue;

      o_cost = getCommisionPip(o, ts);
      if(Double.compare(o_profit - o_cost, step_1st_pip * trail_step_entry_multiplier) <= 0) {
        if(debug && Double.compare(o_profit, step_1st_pip * trail_step_entry_multiplier) > 0)
          SharedProps.print(inst_str +
            " sl-state:" + SharedProps.round(o_profit - o_cost, 2) +
            " pl:" + SharedProps.round(o_profit, 2) +
            " cost:" + SharedProps.round(o_cost, 2) +
            " commision:" + o.getCommission() +
            " value:" + o.getProfitLossInAccountCurrency()
          );
        continue;
      }

      o_profit *= inst_pip;
      double trailingStep = step_1st;

      if(Double.compare(o_profit - trailingStep,  o_profit * trail_step_rest_plus_gain) > 0)
        trailingStep += o_profit * trail_step_rest_plus_gain;

      if(debug)
        SharedProps.print(inst_str + " cost:"+SharedProps.round(o_cost, 2) +
                                     " step:"+SharedProps.round((trailingStep/inst_pip), 2));
      boolean update_sl = false;
      o_sl = getStopLoss(o, trail_managed);
      double trailSLprice = 0.0;

      if (cmd == OrderCommand.BUY) {
        trailSLprice = lastTickBid - trailingStep; // - (lastTickAsk - lastTickBid);
        trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);
        update_sl = Double.compare(trailSLprice, 0.0) > 0 &&
                    Double.compare(o.getOpenPrice(), trailSLprice) < 0 &&
                    (Double.compare(o_sl, 0.0) == 0 ||
                     Double.compare(o_sl, trailSLprice) < 0 ||
                     (!trail_managed && Double.compare(o.getTrailingStep(), 0.0) == 0));

      } else if (cmd == OrderCommand.SELL) {
        trailSLprice = lastTickAsk + trailingStep; // + (lastTickAsk - lastTickBid);
        trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);
        update_sl = Double.compare(trailSLprice, 0.0) > 0 &&
                    Double.compare(o.getOpenPrice(), trailSLprice) > 0 &&
                    (Double.compare(o_sl, 0.0) == 0 ||
                     Double.compare(o_sl, trailSLprice) > 0 ||
                     (!trail_managed && Double.compare(o.getTrailingStep(), 0.0) == 0));
      }
      if(update_sl && Double.compare(trailSLprice, 0.0) > 0) {
        if(debug) {
          SharedProps.print(
            inst_str + " Updating SL" +
            " from=" + SharedProps.round(o_sl, inst_scale + 1) +
            " to=" + SharedProps.round(trailSLprice, inst_scale + 1) +
            " step=" + SharedProps.round(trailingStep, inst_scale + 1) +
            " at=" + SharedProps.round(o_profit/inst_pip, 1)
          );
        }
        orders_trailingstep.put(o.getId(), trailSLprice);
        if(!trail_managed) {
          if(ts - SharedProps.oBusy.get(o.getId()) > 888) {
            if(!reach_mergable && Double.compare(o_sl, 0.0) == 0)
              reach_mergable = true;
            executeTrailOrder(o, trailSLprice, SharedProps.round((trailingStep/inst_pip)*1.2, inst_scale), ts);
          } else {
            reach_mergable = true;
          }
        }
      }

    }


    //// submit Orders
    if(reach_mergable || inst_busy_exec_ts[0].get() > ts || inst_busy_exec_ts[1].get() > ts || inst_busy_exec_ts[2].get() > ts) {
      return;
    }


    if(state_gain_chg.get()) {
      if(inst_starts)
        return;
      double allow =  -(1.5 * offer_diff + configs.equity_gain_state_close_step_multiplier.get() * step_1st_pip);
      if(orders.size() == 1) {
        IOrder o = orders.get(0);
        if(Double.compare(o.getAmount(), amt_one) > 0)
          allow = -1.5 * offer_diff;
        if(Double.compare(getStopLoss(o, trail_managed), 0.0) == 0 &&
           Double.compare(o.getProfitLossInPips(), allow) < 0) {
          SharedProps.print(
            inst_str + " state equity_change closing " + o.getOrderCommand() +
            " amt=" + o.getAmount() + " pip=" + o.getProfitLossInPips() +
            " allow=" + SharedProps.round(allow, 2) +
            " diff=" + SharedProps.round(offer_diff, 2) + " step=" + SharedProps.round(step_1st_pip, 2)
          );
          executeClose(o, 0.0, (o.getOrderCommand() == OrderCommand.BUY ? lastTickBid : lastTickAsk), ts);
          return;
        }
      } else {
        IOrder o1 = orders.get(0);
        IOrder o2 = orders.get(1);
        double amt1 = o1.getAmount() * inst_amt_ratio;
        double amt2 = o2.getAmount() * inst_amt_ratio;
        double pip1 = o1.getProfitLossInPips() * amt1;
        double pip2 = o2.getProfitLossInPips() * amt2;
        double at = (pip1 + pip2) / (amt1 + amt2);
        allow *= 2;
        if(Double.compare(at, allow) < 0) {
          SharedProps.print(
            inst_str + " state equity_change merge " +
            " 1: amt=" + o1.getAmount() + " pip=" + o1.getProfitLossInPips() +
            " 2: amt=" + o2.getAmount() + " pip=" + o2.getProfitLossInPips() +
            " at: pip=" + SharedProps.round(at, 2) + " allow=" + SharedProps.round(allow, 2) +
            " diff=" + SharedProps.round(offer_diff, 2) + " step=" + SharedProps.round(step_1st_pip, 2)
          );
          mergeBuyOrders.add(o1);
          mergeBuyOrders.add(o2);
          executeMergeOrders(mergeBuyOrders, ts);
          return;
        }
      }
      state_gain_chg.set(false);
      return;
    }

    double buying_amt = amt_one;
    double selling_amt = amt_one;
    boolean positiveBuyOrder = false;
    boolean positiveSellOrder = false;

    final double order_zero_base_step_multiplier = configs.order_zero_base_step_multiplier.get();
    final double open_followup_step_muliplier = configs.open_followup_step_muliplier.get();
    final double open_followup_step_first_muliplier = configs.open_followup_step_first_muliplier.get();
    final double open_followup_step_first_upto_ratio = configs.open_followup_step_first_upto_ratio.get();
    final double require_growth = Math.abs((amt_buy - amt_sell)/amt_one) * configs.open_followup_require_growth_rate.get();

    final double open_followup_flat_amt_muliplier = configs.open_followup_flat_amt_muliplier.get();
    final boolean with_flat_muliplier = Double.compare(open_followup_flat_amt_muliplier, 0.0) > 0;

    final boolean neg_zero_base = Double.compare(order_zero_base_step_multiplier, 0.0) <= 0;
    o_cost = offer_diff + Math.abs(step_1st_pip * order_zero_base_step_multiplier);
    if(neg_zero_base)
      o_cost *= -1;

    final double _step_1st_pip = step_1st_pip + (offer_diff / configs.open_followup_step_offers_diff_devider.get());

    for(IOrder o : orders) {
      o_profit = o.getProfitLossInPips();
      o_sl = getStopLoss(o, trail_managed);
      boolean at_sl = Double.compare(o_sl, 0.0) > 0;
      if(!at_sl && Double.compare(o_profit - o_cost, 0.0) < 0)
        continue;

      double require = require_growth;
      double reach = o_profit;
      if(at_sl) {
        require += open_followup_step_muliplier;
        reach *= (1 - trail_step_rest_plus_gain);
        reach -= getCommisionPip(o, ts);
      } else {
        require += open_followup_step_first_muliplier;
      }
      reach -= offer_diff;
      if(!neg_zero_base)
        reach -= offer_diff;
      reach /= _step_1st_pip;

      cmd = o.getOrderCommand();
      if(cmd == OrderCommand.BUY) {
        if(Double.compare(SharedProps.round(amt_sell - amt_buy, inst_amt_scale), inst_amt_min) >= 0)
          continue;
        positiveBuyOrder = true;
        if(Double.compare(reach, require) < 0)
          continue;
        if(!at_sl && Double.compare(amt_sell, 0.0) > 0 &&
           Double.compare(SharedProps.round(amt_buy/amt_sell, 3), open_followup_step_first_upto_ratio) >= 0)
          continue;
        if(at_sl ||
           (trail_at_one_side && Double.compare(amt_sell, 0.0) > 0) ||
           inst_flat ||
           Double.compare(amt_buy, SharedProps.round(amt_sell + amt_one * open_followup_amount_muliplier, inst_amt_scale)) <= 0) {
          SharedProps.print(inst_str + " Reached Flw Open-new Buy " + SharedProps.round(reach, 1) + " > " + SharedProps.round(require, 1));
          --orders_buy;
          if(!at_sl && with_flat_muliplier) {
            double amt_from_flat = amt_sell * open_followup_flat_amt_muliplier - (amt_buy - amt_sell);
            if(Double.compare(amt_from_flat, buying_amt) > 0)
              buying_amt = amt_from_flat;
          }
        }
      } else if(cmd == OrderCommand.SELL) {
        if(Double.compare(SharedProps.round(amt_buy - amt_sell, inst_amt_scale), inst_amt_min) >= 0)
          continue;
        positiveSellOrder = true;
        if(Double.compare(reach, require) < 0)
          continue;
        if(!at_sl && Double.compare(amt_buy, 0.0) > 0 &&
           Double.compare(SharedProps.round(amt_sell/amt_buy, 3), open_followup_step_first_upto_ratio) >= 0)
          continue;
        if(at_sl ||
           (trail_at_one_side && Double.compare(amt_buy, 0.0) > 0) ||
           inst_flat ||
           Double.compare(amt_sell, SharedProps.round(amt_buy + amt_one * open_followup_amount_muliplier, inst_amt_scale)) <= 0) {
          SharedProps.print(inst_str + " Reached Flw Open-new Sell " + SharedProps.round(reach, 1) + " > " + SharedProps.round(require, 1));
          --orders_sell;
          if(!at_sl && with_flat_muliplier) {
            double amt_from_flat = amt_buy * open_followup_flat_amt_muliplier - (amt_sell - amt_buy);
            if(Double.compare(amt_from_flat, selling_amt) > 0)
              selling_amt = amt_from_flat;
          }
        }
      }
    }

    Boolean do_buy =  orders_buy  < 1 && !positiveSellOrder;
    Boolean do_sell = orders_sell < 1 && !positiveBuyOrder;

    if(do_buy || do_sell) {

      long check_ts = (inst_starts ? configs.delays_init_execution(ts) : (inst_exec_ts - ts));
      if(check_ts >= 0) {
        if(log_at_ts)
          SharedProps.print(inst_str + " skip instrument execution" + (inst_starts ? "-init" : "") + " delay=" + check_ts + "ms");
        return;
      }
      inst_exec_ts = ts + configs.execute_inst_delay_ms.get();

      if(Double.compare(SharedProps.get_free_margin(), 0.0) > 0) {
        if(debug) {
          SharedProps.print(
            inst_str +
            " submit do: one=" + amt_one + " buy=" + (do_buy ? buying_amt : 0) + " sell=" + (do_sell ? selling_amt : 0) +
            " total: buy=" + amt_buy + " sell=" + amt_sell
          );
        }
        if(do_buy)
          executeSubmitOrder(OrderCommand.BUY, buying_amt, lastTickAsk, ts);
        if(do_sell)
          executeSubmitOrder(OrderCommand.SELL, selling_amt, lastTickBid, ts);
      }

    } else if(Double.compare(SharedProps.round(Math.abs(amt_buy - amt_sell), inst_amt_scale), inst_amt_min) >= 0) {
      if(!positiveBuyOrder && Double.compare(SharedProps.round(amt_buy - inst_amt_min, inst_amt_scale), amt_sell) >= 0) {
        executeSubmitOrder(OrderCommand.SELL, amt_buy - amt_sell, lastTickBid, ts);
      }
      if(!positiveSellOrder && Double.compare(SharedProps.round(amt_sell - inst_amt_min, inst_amt_scale), amt_buy) >= 0) {
        executeSubmitOrder(OrderCommand.BUY, amt_sell - amt_buy, lastTickAsk, ts);
      }
    }

  }



  //
  private double getCommisionPip(IOrder o, long ts) {
    double value = Math.abs(o.getCommission());
    double swaps = 0.0;
    try {
      long days = (long)(ts / 86400000) - (long)(o.getFillTime() / 86400000);
      if(days > 0) {
        swaps = o.getOrderCommand() == OrderCommand.BUY
          ? dataService.getLongSwapInAccountCurrency(inst, inst_amt_min)
          : dataService.getShortSwapInAccountCurrency(inst, inst_amt_min);
        if(Double.isNaN(swaps))
          swaps = 0.0;
        else
          value += days * (o.getAmount()/inst_amt_min) * Math.abs(swaps);
      }
    } catch(Exception e) { }

    double pl = Math.abs(o.getProfitLossInAccountCurrency());
    double pip = Math.abs(o.getProfitLossInPips());
    double chk = 0.5;
    double rslt = 0.0;
    double rate = 0.0;
    boolean w_chk = Double.compare(pl, 1.0) > 0 && Double.compare(pip, 1.0) > 0;
    if(w_chk)
      chk = SharedProps.round(value/(pl/pip) + 0.1, 1);
    try {
      rate = utils.convertPipToCurrency(
        inst,
        configs.account_currency,
        (o.getOrderCommand() == OrderCommand.BUY ? OfferSide.ASK : OfferSide.BID)
      );
      if(!Double.isNaN(rate) && Double.compare(rate, 0.0) != 0) {
        rate *= o.getAmount() * inst_amt_ratio * (1000); // account_currency == pip-value (0.0001)
        rslt = SharedProps.round(value/rate + 0.1, 1);
      }
    } catch (Exception e) { }

    if(w_chk && configs.debug.get() && Double.compare(rslt, chk) != 0) {
      SharedProps.print(
        inst_str + " getCommisionPip rate:" + SharedProps.round(rate, 1) +
        " value:" + SharedProps.round(value, 2) +
        " swaps:" + SharedProps.round(swaps, 2) +
        " chk:" + chk + " rslt:" + rslt
      );
    }
    return SharedProps.round((Double.compare(rslt, 0.0) == 0 ? chk: rslt) * 2.2, 1);
  }
  private double getStopLoss(IOrder o, boolean trail_managed) {
    return trail_managed
      ? orders_trailingstep.getOrDefault(o.getId(), 0.0)
      : o.getStopLossPrice();
  }
  private void set_busy_exec_ts(OrderCommand cmd, long ts) {
    inst_busy_exec_ts[cmd.isLong() ? 0 : 1].set(ts);
  }
  private String getNextOrderId(OrderCommand cmd) {
    long ms = System.currentTimeMillis();
    long us = System.nanoTime()/1000;
    return cmd.toString().charAt(0) + "" + (
      (ms - ((long)(ms/31536000000L)) * 31536000000L) * 1000 + (us - ((long)(us/1000)) * 1000));
  }

  //
  private void executeTrailOrder(IOrder o, double trailSLprice, double trailingStepPip, long ts) {
    SharedProps.oBusy.put(o.getId(), ts + 3000);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new TrailingOrder(o, trailSLprice, trailingStepPip));
        } catch (Exception e) {
          SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
        }
      }
    }).start();
  }
  private class TrailingOrder implements Callable<IOrder> {
    private final IOrder o;
    private final double trail_price;
    private final double trail_step;

    public TrailingOrder(IOrder order, double price, double step) {
      o = order;
      trail_price = price;
      trail_step = Double.compare(step, 10.0) > 0 ? step : (Double.compare(step, 0.0) == 0 ? 0: 10);
    }
    public IOrder call() {
      try {
        if(o.getOrderCommand() == OrderCommand.BUY) {
          o.setStopLossPrice(trail_price, OfferSide.BID, trail_step);
          if (Double.compare(o.getTakeProfitPrice(), 0.0) != 0 && Double.compare(o.getOpenPrice(), trail_price) < 0)
            o.setTakeProfitPrice(0);

        } else if(o.getOrderCommand() == OrderCommand.SELL) {
          o.setStopLossPrice(trail_price, OfferSide.ASK, trail_step);
          if (Double.compare(o.getTakeProfitPrice(), 0.0) != 0 && Double.compare(o.getOpenPrice(), trail_price) > 0)
            o.setTakeProfitPrice(0);
        }
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 900);
      } catch (Exception e) {
        SharedProps.print(inst_str + " TrailingOrder E: " + e + " Thread: " + Thread.currentThread().getName());
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
      }
      return o;
    }
  }

  //
  private void executeMergeOrders(List<IOrder> orders, long ts) {
    for(IOrder o : orders) {
      SharedProps.oBusy.put(o.getId(), ts + 900);
    }
    inst_busy_exec_ts[2].set(ts + 10000);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new MergeOrders(orders));
        } catch (Exception e) {
          for(IOrder o : orders) {
            SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
          }
          inst_busy_exec_ts[2].set(0);
        }
      }
    }).start();
  }
  private class MergeOrders implements Callable<IOrder> {
    private final List<IOrder> orders;
    final OrderCommand         cmd;
    public MergeOrders(List<IOrder> _orders) {
      orders = _orders;
      IOrder o1 = orders.get(0);
      IOrder o2 = orders.get(1);
      cmd = (o1.getOrderCommand() == o2.getOrderCommand()
              ? o1.getOrderCommand()
              : (Double.compare(o1.getProfitLossInPips(), o2.getProfitLossInPips()) > 0
                  ? o1.getOrderCommand() : o2.getOrderCommand()));
    }
    public IOrder call() {
      IOrder o=null;
      long ts = SharedProps.get_sys_ts();
      try {
        o = engine.mergeOrders(getNextOrderId(cmd), orders.get(0), orders.get(1));
        if(o != null && o.getId() != null)
          SharedProps.oBusy.put(o.getId(), ts + 900);

      } catch (Exception e) {
        SharedProps.print(inst_str + " MergeOrders E: " + e + " Thread: " + Thread.currentThread().getName());
        for(IOrder _o : orders) {
          SharedProps.oBusy.put(_o.getId(), ts - 1000);
        }
        inst_busy_exec_ts[2].set(0);
      }
      return o;
    }
  }

  //
  private void executeOrderSetTP(IOrder o, double tp_price, long ts) {
    SharedProps.oBusy.put(o.getId(), ts + 900);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new OrderSetTP(o, tp_price));
        } catch (Exception e) {
          SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
        }
      }
    }).start();
  }
  private class OrderSetTP implements Callable<IOrder> {
    private final double tp;
    private final IOrder o;

    public OrderSetTP(IOrder order, double tp_price) {
      o = order;
      tp = tp_price;
    }
    public IOrder call() {
      try {
        o.setTakeProfitPrice(tp);
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 900);
      } catch (Exception e) {
        SharedProps.print(inst_str + " OrderSetTP E: " + e + " Thread: " + Thread.currentThread().getName());
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
      }
      return o;
    }
  }

  //
  private void executeOrderSetSL(IOrder o, double sl_price, long ts) {
    SharedProps.oBusy.put(o.getId(), ts + 900);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new OrderSetSL(o, sl_price));
        } catch (Exception e) {
          SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
        }
      }
    }).start();
  }
  private class OrderSetSL implements Callable<IOrder> {
    private final double sl;
    private final IOrder o;

    public OrderSetSL(IOrder order, double sl_price) {
      o = order;
      sl = sl_price;
    }
    public IOrder call() {
      try {
        o.setStopLossPrice(sl);
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 900);
      }catch (Exception e){
        SharedProps.print(inst_str + " OrderSetSL E: " + e + " Thread: " + Thread.currentThread().getName());
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
      }
      return o;
    }
  }

  //
  private void executeClose(IOrder o, double _amt, double _price, long ts) {
    SharedProps.oBusy.put(o.getId(), ts + 4000);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new CloseProceed(o, _amt, _price));
        } catch (Exception e) {
          SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
        }
      }
    }).start();
  }
  private class CloseProceed implements Callable<IOrder> {
    private final IOrder o;
    private final double amt;
    private final double price;

    public CloseProceed(IOrder order, double _amt, double _price) {
      o = order;
      amt = _amt;
      price = _price;
    }
    public IOrder call() {
      try {
        o.close(amt); // , price, 0.1
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 900);
      } catch (Exception e) {
        SharedProps.print(inst_str + " CloseProceed E: " + e + " Thread: " + Thread.currentThread().getName());
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
      }
      return o;
    }
  }

  //
  private void executeSubmitOrder(OrderCommand cmd, double amount, double at, long ts) {
    set_busy_exec_ts(cmd, ts + 10000);
    final double amt = SharedProps.round(amount, inst_amt_scale + 3);
    SharedProps.print(inst_str + " submit " + cmd + " open amt=" + amt + " at:" + at);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new SubmitOrderProceed(cmd, amt, at));
        } catch (Exception e) {
          set_busy_exec_ts(cmd, 0);
        }
      }
    }).start();
  }
  private class SubmitOrderProceed implements Callable<IOrder> {
    private final double amt, at;
    private final OrderCommand cmd;

    public SubmitOrderProceed(OrderCommand _cmd, double _amt, double _at) {
      cmd = _cmd;
      amt = _amt;
      at = _at;
    }
    public IOrder call() {
      IOrder o = null;
      try {
        o = engine.submitOrder(getNextOrderId(cmd), inst, cmd, amt);
        if(o != null && o.getId() != null) {
          SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 900);
          SharedProps.print(inst_str + " submit " + cmd + " open amt=" + amt + " at:" + at + " id:" + o.getId());
        }
      } catch (Exception e) {
        SharedProps.print(inst_str + " SubmitOrderProceed E: " + e + " Thread: " + Thread.currentThread().getName());
        set_busy_exec_ts(cmd, 0);
      }
      return o;
    }
  }


  //
  private ITick getLastTick() {
    ITick lastTick = null;
    do {
      try{
        lastTick = history.getLastTick(inst);
        if(lastTick != null && !Double.isNaN(lastTick.getAsk()) && !Double.isNaN(lastTick.getBid()))
          break;
      } catch (Exception e) { }
      try{ Thread.sleep(2); } catch (Exception e) { }
    } while(!stop_run.get());
    return lastTick;
  }
  private double getMin(double time_frame, OfferSide side){
    try {
      Period p = configs.minutes_to_period(time_frame);
      long to = history.getBarStart(p, getLastTick().getTime());
      long from = to - p.getInterval()*configs.minutes_to_period_scale(time_frame);
      List<IBar> bars = history.getBars(inst, p, side, from, to);
      double min = Double.MAX_VALUE;
      double v;
      for (IBar b: bars) {
        v = b.getLow();
        if(Double.compare(v, min)<0) min = v;
      }
      if(Double.compare(min, Double.MAX_VALUE)<0)
        return min;
    }catch (Exception e){
      if(configs.debug.get())
        SharedProps.print(inst_str + " getMin E: @" + time_frame + " " + e + " Thread: " + Thread.currentThread().getName());
    }
    return Double.NaN;
  }
  private double getMax(double time_frame, OfferSide side){
    try {
      Period p = configs.minutes_to_period(time_frame);
      long to = history.getBarStart(p, getLastTick().getTime());
      long from = to - p.getInterval()*configs.minutes_to_period_scale(time_frame);
      List<IBar> bars = history.getBars(inst, p, side, from, to);
      double max = Double.MIN_VALUE;
      double v;
      for (IBar b: bars) {
        v = b.getHigh();
        if(Double.compare(v, max)>0) max = v;
      }
      if(Double.compare(max, Double.MIN_VALUE)>0)
        return max;
    }catch (Exception e){
      if(configs.debug.get())
        SharedProps.print(inst_str + " getMax E: @" + time_frame + " " + e + " Thread: " + Thread.currentThread().getName());
    }
    return Double.NaN;
  }

  private double get_offer_diff(long tick_ts, double diff) {
    double max = 0.0;
    if(inst_offer_diff_ts > tick_ts) {
      max = inst_offer_diff;
    } else {
      try {
        for(ITick tick : history.getTicks(inst, tick_ts - 24 * 3600000, tick_ts)) {
          double v = tick.getAsk() - tick.getBid();
          if(Double.compare(max, v) < 0)
            max = v;
        }
        if(Double.compare(max, 0.0) > 0) {
          inst_offer_diff = max;
          inst_offer_diff_ts = ((long)(tick_ts/3600000))*3600000 + 3600000;
        }
      } catch(Exception e) {
        max = inst_offer_diff;
      }
    }
    if(Double.compare(max, diff) < 0) {
      inst_offer_diff = diff;
      return diff;
    }
    return (max + diff)/2;
  }


}
