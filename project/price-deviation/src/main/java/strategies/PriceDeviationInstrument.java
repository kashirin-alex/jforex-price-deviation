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


public class PriceDeviationInstrument implements IStrategy {

  public AtomicBoolean stop_run = new AtomicBoolean(false);
  public AtomicBoolean pid_restart = new AtomicBoolean(false);

  private IEngine engine;
  private IHistory history;
  private IContext context;
  private IAccount account;
  private IIndicators indicators;

  private StrategyConfigs configs;
  public IClient client;
  public long strategyId;
  public Instrument inst;
  public Thread inst_thread;

  private String inst_str;
  private String inst_cur_1;
  private String inst_cur_2;
  private double inst_pip;
  private int inst_scale;
  private AtomicBoolean inst_init = new AtomicBoolean(false);

  private AtomicLong inst_exec_ts = new AtomicLong(0);
  private AtomicBoolean inst_busy = new AtomicBoolean(false);
  private AtomicLong inst_busy_ts = new AtomicLong(SharedProps.get_sys_ts());
  private AtomicLong inst_busy_exec_ts = new AtomicLong(inst_busy_ts.get());
  private AtomicInteger inst_tick_count = new AtomicInteger(0);

  private double minFirstStepByInst;
  private long min_1st_step_inst_ts = 0;

  private ConcurrentHashMap<String, Double> orders_trailingstep = new ConcurrentHashMap<>();

  @Override
  public void onAccount(IAccount account) {}

  @Override
  public void onStart(IContext ctx) {

    SharedProps.print("onStart start");
    configs = SharedProps.configs;

    context = ctx;
    engine = ctx.getEngine();
    history = ctx.getHistory();
    account = ctx.getAccount();
    indicators = ctx.getIndicators();
    inst_str = inst.toString();
    inst_pip = inst.getPipValue();
    inst_scale = inst.getPipScale();

    inst_cur_1 = inst.getPrimaryJFCurrency().getCurrencyCode();
    inst_cur_2 = inst.getSecondaryJFCurrency().getCurrencyCode();

    context.setSubscribedInstruments(java.util.Collections.singleton(inst), true);

    minFirstStepByInst = configs.trail_step_1st_min * inst_pip;

    new Thread(new Runnable() {
      @Override
      public void run() {runInstrument();}
    }).start();
  }


  private void runInstrument() {
    int counter = 999;
    long ts = SharedProps.get_sys_ts();
    inst_exec_ts.set(ts + 10000);
    while(!stop_run.get()) {
      try{
        if(!SharedProps.inst_active.get(inst_str)) {
          pid_restart.set(false);
          Thread.sleep(10000);
          continue;
        }
        Thread.sleep(configs.execute_inst_ms);
      }catch (Exception e){ }

      ts = SharedProps.get_sys_ts();

      if(ts - min_1st_step_inst_ts > 60000)
        setInstFirstStep(ts);

      if(configs.debug && ++counter == 1000) {
        SharedProps.print(inst_str +
              "|Amt: " + getAmount(1) +
              "|1stStep: " + SharedProps.round(getFirstStep()/inst_pip, 1) +
              "|min1stStep: " + SharedProps.round(minFirstStepByInst,1) +
              "|diff: " + SharedProps.round(getOffersDifference()/inst_pip, 1)
        );
        counter = 0;
      }

      if(ts < inst_exec_ts.get())
        continue;
      if(inst_busy_exec_ts.get() > ts)
        continue;

      try {
        executeInstrument(ts);
      } catch (Exception e) {
        SharedProps.print("executeInstrument E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }

      pid_restart.set(false);
      inst_init.set(true);
    }
  }

  @Override
  public void onTick(final Instrument instrument, ITick tick) {
    if(!instrument.toString().equals(inst_str)) return;
    pid_restart.set(false);

    if(inst_busy.get()) {
      inst_tick_count.getAndIncrement();
      long ts = SharedProps.get_sys_ts();
      if (ts - inst_busy_ts.get() > 30000) {
        if(configs.debug)
          SharedProps.print(inst_str + " busy: " + (ts - inst_busy_ts.get()) + " ticks behind: " + inst_tick_count.get());
        inst_tick_count.set(0);

        if (ts - inst_busy_ts.get() > 300000) {
          if (inst_thread != null) {
            try {
              inst_thread.interrupt();
            } catch (Exception e) {
            }
            inst_thread = null;
          }
          inst_busy.set(false);
        }
      }
    }
    executeManageInstrument(0);
  }

  private void executeManageInstrument(final int calls) {
    if(!inst_busy.compareAndSet(false, true))
      return;
    inst_tick_count.set(0);
    inst_busy_ts.set(SharedProps.get_sys_ts());
    inst_thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          manageInstrument();
        } catch (Exception e) {
          SharedProps.print("ManageInstOrders call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
        }
        inst_busy.set(false);
        inst_thread = null;
        if(calls < 2) {
          try { Thread.sleep(1000); } catch (Exception e) { }
          executeManageInstrument(calls + 1);
        }
      }
    });
    inst_thread.start();
  }


  //
  private void manageInstrument() throws Exception {
    List<IOrder> orders_raw = new ArrayList<>();
    List<IOrder> orders = new ArrayList<>();
    List<IOrder> mergeBuyOrders = new ArrayList<>();
    List<IOrder> mergeSellOrders = new ArrayList<>();

    OrderCommand cmd;
    long ts = SharedProps.get_sys_ts();

    double o_profit, lastTickBid, lastTickAsk, trailSLprice, trailingStep;
    ITick lastTick;

    double amt_buy = 0.0;
    double amt_sell = 0.0;

    for(IOrder o : engine.getOrders(inst)) {
      cmd = o.getOrderCommand();
      if(cmd == OrderCommand.BUY || cmd == OrderCommand.PLACE_BID || cmd == OrderCommand.BUYLIMIT || cmd == OrderCommand.BUYSTOP) {
        amt_buy += o.getAmount();
      } else if(cmd == OrderCommand.SELL || cmd == OrderCommand.PLACE_OFFER || cmd == OrderCommand.SELLLIMIT || cmd == OrderCommand.SELLSTOP) {
        amt_sell += o.getAmount();
      }
      if(o.getState() == IOrder.State.FILLED && (cmd == OrderCommand.BUY || cmd == OrderCommand.SELL)) {
        if(configs.trail_managed) {
          orders_raw.add(o);
        } else {
          orders.add(o);
        }
        if(!SharedProps.oBusy.containsKey(o.getId()))
          SharedProps.oBusy.put(o.getId(), ts);
      }
    }
    amt_buy = SharedProps.round(amt_buy, 3);
    amt_sell = SharedProps.round(amt_sell, 3);

    double step_1st = getFirstStep();
    double step_1st_pip = step_1st/inst_pip;

    for(IOrder o : orders_raw) {
      double sl = orders_trailingstep.getOrDefault(o.getId(), 0.0);
      if(Double.compare(sl, 0.0) > 0) {
        if(ts - SharedProps.oBusy.get(o.getId()) < 888)
          continue;
        lastTick = getLastTick();
        double diff = 0.0;
        double amt = 0.0;
        double price = 0.0;
        boolean closing = false;
        boolean clear = false;
        if(o.getOrderCommand() == OrderCommand.BUY && Double.compare(amt_buy, amt_sell) > 0) {
          price = lastTick.getBid();
          diff = sl - price;
          if(Double.compare(diff, 0.0) >= 0) {
            if(Double.compare(diff, step_1st) < 0) {
              closing = true;
              amt = 0.0;
              amt_buy -= o.getAmount();
            } else {
              clear = true;
            }
          }
        } else if(o.getOrderCommand() == OrderCommand.SELL && Double.compare(amt_sell, amt_buy) > 0) {
          price = lastTick.getAsk();
          diff = price - sl;
          if(Double.compare(diff, 0.0) >= 0) {
            if(Double.compare(diff, step_1st) < 0) {
              closing = true;
              amt = 0.0;
              amt_sell -= o.getAmount();
            } else {
              clear = true;
            }
          }
        }
        if(clear) {
          orders_trailingstep.remove(o.getId());
        } else if(closing) {
          amt = SharedProps.round(amt, 6);
          SharedProps.oBusy.put(o.getId(), ts + 4000);
          SharedProps.print(
            inst_str + " Closing SL at price=" + price + " sl=" + sl +
            " diff=" + SharedProps.round(diff/inst_pip, 2) +
            " amt=" + amt + " " + o
          );
          executeClose(o, amt, price);
          continue;
        }
      }
      orders.add(o);
    }
    amt_buy = SharedProps.round(amt_buy, 3);
    amt_sell = SharedProps.round(amt_sell, 3);

    if(inst_init.get() && orders.size() > 1) {
      mergeBuyOrders.addAll(orders);
      mergeBuyOrders.sort(new Comparator<IOrder>() {
        @Override
        public int compare(IOrder lhs, IOrder rhs) {
          return Double.compare(lhs.getProfitLossInPips(), rhs.getProfitLossInPips()) > 0 ? -1 : 1;
        }
      });
      IOrder pos = mergeBuyOrders.get(0);
      IOrder neg = mergeBuyOrders.get(mergeBuyOrders.size()-1);
      int neg_count = 0;
      for(IOrder o : mergeBuyOrders) {
        if(neg.getOrderCommand() == o.getOrderCommand() && ++neg_count > 1)
          break;
      }
      mergeBuyOrders.clear();
      if(neg_count > 1 || ts - SharedProps.oBusy.get(neg.getId()) < 888)
        neg_count = 0;

      if(neg_count == 1 &&
         pos.getOrderCommand() != neg.getOrderCommand() &&
         Double.compare(pos.getProfitLossInAccountCurrency(), Math.abs(neg.getProfitLossInAccountCurrency())) > 0 &&
         Double.compare(pos.getAmount(), neg.getAmount()) > 0) {
        double sl = configs.trail_managed
          ? orders_trailingstep.getOrDefault(pos.getId(), 0.0)
          : pos.getStopLossPrice();
        if(Double.compare(sl, 0.0) > 0) {
          double pos_dif, neg_dif;
          lastTick = getLastTick();
          double price;
          if(pos.getOrderCommand() == OrderCommand.BUY) {
            pos_dif = sl - pos.getOpenPrice();
            price = lastTick.getBid();
            neg_dif = price - neg.getOpenPrice();
          } else {
            pos_dif = pos.getOpenPrice() - sl;
            price = lastTick.getAsk();
            neg_dif = neg.getOpenPrice() - price;
          }
          pos_dif *= (pos.getAmount()*1000);
          neg_dif *= (neg.getAmount()*1000);
          pos_dif -= getCommisionPip(pos) * inst_pip;
          neg_dif -= getCommisionPip(neg) * inst_pip;
          pos_dif -= Math.abs((getCommisionPip(pos) - getCommisionPip(neg)) * inst_pip);

          double reach = (pos_dif - neg_dif) / ((pos.getAmount() - neg.getAmount()) * 1000);
          boolean merge = Double.compare(reach, step_1st * configs.merge_close_neg_side_multiplier) > 0;
          if(merge || configs.debug) {
            SharedProps.print(
              inst_str + " Merge " + (merge ? "" : "nominated ") + "pos-close" +
              " pos_dif=" + SharedProps.round(pos_dif, inst_scale + 2) +
              " neg_dif=" + SharedProps.round(neg_dif, inst_scale + 2) +
              " reach=" + SharedProps.round(reach, inst_scale + 2) +
              " @ " + pos.getProfitLossInAccountCurrency() + " > " + neg.getProfitLossInAccountCurrency()
            );
          }
          if(merge) {
            SharedProps.oBusy.put(neg.getId(), ts + 4000);
            executeClose(neg, 0, price);
            orders.remove(neg);
          }
        } else {
          double pos_dif = pos.getProfitLossInPips() * (pos.getAmount()*1000) - getCommisionPip(pos);
          double neg_dif = Math.abs(neg.getProfitLossInPips()) * (neg.getAmount()*1000) + getCommisionPip(neg);
          pos_dif -= Math.abs(getCommisionPip(pos) - getCommisionPip(neg));

          double reach = (pos_dif - neg_dif) / ((pos.getAmount() - neg.getAmount()) * 1000);
          boolean merge = Double.compare(reach, step_1st_pip * configs.merge_close_neg_side_multiplier) > 0;
          if(merge || configs.debug) {
            SharedProps.print(
              inst_str + " Merge " + (merge ? "" : "nominated ") + "on side" +
              " pos_dif=" + SharedProps.round(pos_dif, 2) +
              " neg_dif=" + SharedProps.round(neg_dif, 2) +
              " reach=" + SharedProps.round(reach, 2) +
              " @ " + pos.getProfitLossInAccountCurrency() + " > " + neg.getProfitLossInAccountCurrency()
            );
          }
          if(merge) {
            mergeBuyOrders.add(neg);
            mergeBuyOrders.add(pos);
            orders.clear();
            SharedProps.oBusy.put(neg.getId(), ts + 500);
            SharedProps.oBusy.put(pos.getId(), ts + 500);
          }
        }
      }
    }

    if(inst_init.get() && orders.size() > 1) {
      for(IOrder o : orders) {
        if(o.getOrderCommand() == OrderCommand.SELL)
          mergeSellOrders.add(o);
        else
          mergeBuyOrders.add(o);
      }
      if(mergeSellOrders.size() < 2) {
        mergeSellOrders.clear();
      } else {
        SharedProps.print(inst_str + " Merge sell");
        orders.clear();
      }
      if(mergeBuyOrders.size() < 2) {
        mergeBuyOrders.clear();
      } else {
        SharedProps.print(inst_str + " Merge buy");
        orders.clear();
      }
    }

    ////
    double amt_one = getAmount(1);
    for( IOrder o : orders) {
      if(Double.compare(o.getProfitLossInPips(), step_1st_pip) <= 0 ||
         ts - SharedProps.oBusy.get(o.getId()) < 888) {
        continue;
      }
      cmd = o.getOrderCommand();
      if(configs.trail_at_one_side
          ? (Double.compare(amt_buy, 0.0) > 0 && Double.compare(amt_sell, 0.0) > 0)
          : (Double.compare(amt_sell, amt_buy) == 0 || (cmd == OrderCommand.BUY
            ? (Double.compare(amt_sell, 0.0) > 0 &&
               Double.compare(amt_buy, SharedProps.round(amt_sell + amt_one * configs.open_followup_amount_muliplier, 3)) <= 0)
            : (Double.compare(amt_buy,  0.0) > 0 &&
               Double.compare(amt_sell, SharedProps.round(amt_buy + amt_one * configs.open_followup_amount_muliplier, 3)) <= 0) ) ) )
        continue;

      trailingStep = step_1st;
      o_profit = o.getProfitLossInPips() * inst_pip;
      double o_cost = getCommisionPip(o);
      if(Double.compare(o_profit - o_cost * inst_pip, trailingStep * configs.trail_step_entry_multiplier) <= 0) {
        if(configs.debug && Double.compare(o_profit,  trailingStep * configs.trail_step_entry_multiplier) > 0)
          SharedProps.print(inst_str +
            " sl-state:" + SharedProps.round(o_profit/inst_pip - o_cost, 2) +
            " pl:" + SharedProps.round(o_profit/inst_pip, 2) +
            " cost:" + SharedProps.round(o_cost, 2) +
            " commision:" + o.getCommission() +
            " value:" + o.getProfitLossInAccountCurrency()
          );
        continue;
      }

      if(Double.compare(o_profit - trailingStep,  o_profit * configs.trail_step_rest_plus_gain) > 0)
        trailingStep += o_profit * configs.trail_step_rest_plus_gain;

      if(configs.debug)
        SharedProps.print(inst_str + " cost:"+SharedProps.round(o_cost, 2) +
                                     " step:"+SharedProps.round((trailingStep/inst_pip), 2));
      lastTick = getLastTick();
      boolean update_sl = false;
      trailSLprice = 0.0;
      if (cmd == OrderCommand.BUY) {
        lastTickBid = lastTick.getBid();
        trailSLprice = lastTickBid - trailingStep; // - (lastTickAsk - lastTickBid);
        trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);

        if (Double.compare(trailSLprice, 0.0) > 0 && Double.compare(o.getOpenPrice(), trailSLprice) < 0) {
          if(configs.trail_managed) {
            double sl = orders_trailingstep.getOrDefault(o.getId(), 0.0);
            update_sl = Double.compare(sl, 0.0) == 0;
            if(!update_sl)
              update_sl = Double.compare(sl, trailSLprice) < 0;
          } else {
            if (Double.compare(o.getStopLossPrice(), trailSLprice) < 0 || Double.compare(o.getStopLossPrice(), 0.0) == 0) {
              if (Double.compare(o.getTrailingStep(), 0.0) == 0 || Double.compare(o.getStopLossPrice(), trailSLprice) < 0)
                update_sl = true;
            }
          }
        }

      } else if (cmd == OrderCommand.SELL) {
        lastTickAsk = lastTick.getAsk();
        trailSLprice = lastTickAsk + trailingStep; // + (lastTickAsk - lastTickBid);
        trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);

        if (Double.compare(trailSLprice, 0.0) > 0 && Double.compare(o.getOpenPrice(), trailSLprice) > 0) {
          if(configs.trail_managed) {
            double sl = orders_trailingstep.getOrDefault(o.getId(), 0.0);
            update_sl = Double.compare(sl, 0.0) == 0;
            if(!update_sl)
              update_sl = Double.compare(sl, trailSLprice) > 0;
          } else {
            if (Double.compare(o.getStopLossPrice(), trailSLprice) > 0 || Double.compare(o.getStopLossPrice(), 0.0) == 0) {
              if (Double.compare(o.getTrailingStep(), 0.0) == 0 || Double.compare(o.getStopLossPrice(), trailSLprice) > 0)
                update_sl = true;
            }
          }
        }
      }
      if(update_sl && Double.compare(trailSLprice, 0.0) > 0) {
        if(configs.debug) {
          SharedProps.print(
            inst_str + " Updating SL to=" +
            SharedProps.round(trailSLprice, inst_scale + 1) +
            " step=" + SharedProps.round(trailingStep, inst_scale + 1) +
            " " + o
          );
        }
        orders_trailingstep.put(o.getId(), trailSLprice);
        if(!configs.trail_managed) {
          executeTrailOrder(cmd, o, trailSLprice, SharedProps.round((trailingStep/inst_pip)*1.2, inst_scale));
        }
      }

    }


    ////
    boolean merge = true;
    if(mergeBuyOrders.size() >= 2) {
      for(IOrder o: mergeBuyOrders){
        if(ts + 500 < SharedProps.oBusy.get(o.getId())) {
          merge = false;
          break;
        }
        if(Double.compare(o.getStopLossPrice(), 0.0) > 0){
          executeOrderSetSL(o, 0);
          merge = false;
        }
      }
      if(merge) {
        executeMergeOrders(mergeBuyOrders);
      }
    }

    merge = true;
    if(mergeSellOrders.size() >= 2) {
      for(IOrder o: mergeSellOrders){
        if(ts + 500 < SharedProps.oBusy.get(o.getId())) {
          merge = false;
          break;
        }
        if(Double.compare(o.getStopLossPrice(), 0.0) > 0){
          executeOrderSetSL(o, 0);
          merge = false;
        }
      }
      if(merge) {
        executeMergeOrders(mergeSellOrders);
      }
    }

  }

  //
  private void executeTrailOrder(OrderCommand cmd, IOrder o, double trailSLprice, double trailingStepPip ){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new TrailingOrder(cmd, o, trailSLprice, trailingStepPip));
        } catch (Exception e) {
        }
      }
    }).start();
  }
  private class TrailingOrder implements Callable<IOrder> {
    private final OrderCommand cmd;
    private final IOrder o;
    private final double trail_price;
    private final double trail_step;

    public TrailingOrder(OrderCommand c, IOrder order, double price, double step) {
      trail_price = price;
      o = order;
      trail_step = Double.compare(step, 10.0) > 0 ? step : (Double.compare(step, 0.0) == 0 ? 0: 10);
      cmd = c;
    }
    public IOrder call() {
      try {
        if (cmd == OrderCommand.BUY) {
          o.setStopLossPrice(trail_price, OfferSide.BID, trail_step);
          if (Double.compare(o.getTakeProfitPrice(), 0.0) != 0 && Double.compare(o.getOpenPrice(), trail_price) < 0)
            o.setTakeProfitPrice(0);

        } else if (cmd == OrderCommand.SELL) {
          o.setStopLossPrice(trail_price, OfferSide.ASK, trail_step);
          if (Double.compare(o.getTakeProfitPrice(), 0.0) != 0 && Double.compare(o.getOpenPrice(), trail_price) > 0)
            o.setTakeProfitPrice(0);
        }
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 900);
      }catch (Exception e){
        SharedProps.print("TrailingOrder call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      return o;
    }
  }

  //
  private void executeMergeOrders(List<IOrder> o_s){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new MergeOrders(o_s));
        } catch (Exception e) {
        }
      }
    }).start();
  }
  private class MergeOrders implements Callable<IOrder> {
    private final List<IOrder> orders;
    public MergeOrders(List<IOrder> o_s) {
      orders = o_s;
    }
    public IOrder call() {
      IOrder om=null;
      try {
        om = engine.mergeOrders(
          orders.get(0).getOrderCommand().toString()+SharedProps.get_sys_ts(),
          orders.get(0), orders.get(1)
        );
        if(om != null && om.getId() != null)
          SharedProps.oBusy.put(om.getId(), SharedProps.get_sys_ts() + 900);

      }catch (Exception e){
        SharedProps.print("MergeOrders call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      return om;
    }
  }

  //
  private void executeOrderSetTP(IOrder o, double tp_price){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new OrderSetTP(o, tp_price));
        } catch (Exception e) {
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
      }catch (Exception e){
        SharedProps.print("OrderSetTP call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      return o;
    }
  }

  //
  private void executeOrderSetSL(IOrder o, double sl_price){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new OrderSetSL(o, sl_price));
        } catch (Exception e) {
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
        SharedProps.print("OrderSetSL call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      return o;
    }
  }


  //
  private void executeInstrument(long ts) throws Exception {

    double amt = getAmount(1);
    double buying_amt = amt;
    double selling_amt = amt;
    double step_1st_pip = getFirstStep()/inst_pip;

    boolean positiveBuyOrder = false;
    boolean positiveSellOrder = false;
    int orders_buy = 0;
    int orders_sell = 0;
    double amt_buy = 0.0;
    double amt_sell = 0.0;

    OrderCommand cmd;
    List<IOrder> orders = new ArrayList<>();
    for(IOrder o : engine.getOrders(inst)) {
      if(o.getState() != IOrder.State.FILLED &&
         o.getState() != IOrder.State.OPENED &&
         o.getState() != IOrder.State.CREATED) continue;
      cmd = o.getOrderCommand();
      if(cmd == OrderCommand.BUY || cmd == OrderCommand.PLACE_BID || cmd == OrderCommand.BUYLIMIT || cmd == OrderCommand.BUYSTOP) {
        ++orders_buy;
        amt_buy += o.getAmount();
        if(cmd == OrderCommand.BUY)
          orders.add(o);
      } else if(cmd == OrderCommand.SELL || cmd == OrderCommand.PLACE_OFFER || cmd == OrderCommand.SELLLIMIT || cmd == OrderCommand.SELLSTOP) {
        ++orders_sell;
        amt_sell += o.getAmount();
        if(cmd == OrderCommand.SELL)
          orders.add(o);
      }
    }
    amt_buy = SharedProps.round(amt_buy, 3);
    amt_sell = SharedProps.round(amt_sell, 3);

    if(orders.size() > 1) {
      List<IOrder> mergeOrders = new ArrayList<>();
      mergeOrders.addAll(orders);
      mergeOrders.sort(new Comparator<IOrder>() {
        @Override
        public int compare(IOrder lhs, IOrder rhs) {
          return Double.compare(lhs.getProfitLossInPips(), rhs.getProfitLossInPips()) > 0 ? -1 : 1;
        }
      });
      IOrder pos = mergeOrders.get(0);
      IOrder neg = mergeOrders.get(mergeOrders.size()-1);
      int neg_count = 0;
      for(IOrder o : mergeOrders) {
        if(neg.getOrderCommand() == o.getOrderCommand() && ++neg_count > 1)
          break;
      }
      if(neg_count == 1 &&
         pos.getOrderCommand() != neg.getOrderCommand() &&
         Double.compare(pos.getProfitLossInAccountCurrency(), Math.abs(neg.getProfitLossInAccountCurrency())) > 0 &&
         Double.compare(pos.getAmount(), neg.getAmount()) > 0) {
        double sl = configs.trail_managed
          ? orders_trailingstep.getOrDefault(pos.getId(), 0.0)
          : pos.getStopLossPrice();
        if(Double.compare(sl, 0.0) == 0) {
          double pos_dif = pos.getProfitLossInPips() * (pos.getAmount()*1000);// - getCommisionPip(pos);
          double neg_dif = Math.abs(neg.getProfitLossInPips()) * (neg.getAmount()*1000);// + getCommisionPip(neg);
          //pos_dif -= Math.abs(getCommisionPip(pos) - getCommisionPip(neg));
          double reach = (pos_dif - neg_dif) / ((pos.getAmount() - neg.getAmount()) * 1000);
          if(Double.compare(reach, step_1st_pip * configs.merge_close_neg_side_multiplier) >= 0) {
            return;
          }
        }
      }
    }

    double o_diff = getOffersDifference();
    double o_cost = o_diff/inst_pip + Math.abs(step_1st_pip * configs.positive_order_step_multiplier);
    if(Double.compare(configs.positive_order_step_multiplier, 0.0) < 0)
      o_cost *= -1;

    for(IOrder o : orders) {
      double o_profit = o.getProfitLossInPips();
      double sl = configs.trail_managed
        ? orders_trailingstep.getOrDefault(o.getId(), 0.0)
        : o.getStopLossPrice();
      boolean at_sl = Double.compare(sl, 0.0) > 0;
      if(!at_sl && Double.compare(o_profit + o_cost, 0.0) < 0)
        continue;

      double require;
      double reach = o_profit;
      if(at_sl) {
        require = configs.open_followup_step_muliplier + (Math.abs((amt_buy - amt_sell)/amt) * configs.open_followup_require_growth_rate);
        reach *= (1 - configs.trail_step_rest_plus_gain);
        reach -= getCommisionPip(o);
      } else {
        require = configs.open_followup_step_first_muliplier;
      }
      reach -= o_diff;
      if(Double.compare(configs.positive_order_step_multiplier, 0.0) < 0)
        reach -= o_diff;
      reach /= step_1st_pip;

      cmd = o.getOrderCommand();
      if(cmd == OrderCommand.BUY) {
        if(Double.compare(amt_buy, amt_sell) < 0)
          continue;
        positiveBuyOrder = true;
        if(Double.compare(reach, require) < 0)
          continue;
        if(at_sl ||
           (configs.trail_at_one_side && Double.compare(amt_sell, 0.0) > 0) ||
           Double.compare(amt_buy, amt_sell) == 0 ||
           Double.compare(amt_buy, SharedProps.round(amt_sell + amt * configs.open_followup_amount_muliplier, 3)) <= 0) {
          SharedProps.print(inst_str +  " Reached Flw Open-new Buy " + reach + " > " + require);
          --orders_buy;
          //if(!at_sl)
          //  buying_amt += (amt_sell + amt * configs.open_followup_amount_muliplier) - amt_buy;
        }
      } else if(cmd == OrderCommand.SELL) {
        if(Double.compare(amt_sell, amt_buy) < 0)
          continue;
        positiveSellOrder = true;
        if(Double.compare(reach, require) < 0)
          continue;
        if(at_sl ||
           (configs.trail_at_one_side && Double.compare(amt_buy, 0.0) > 0) ||
           Double.compare(amt_buy, amt_sell) == 0 ||
           Double.compare(amt_sell, SharedProps.round(amt_buy + amt * configs.open_followup_amount_muliplier, 3)) <= 0) {
          SharedProps.print(inst_str +  " Reached Flw Open-new Sell " + reach + " > " + require);
          --orders_sell;
          //if(!at_sl)
          //  selling_amt += (amt_buy + amt * configs.open_followup_amount_muliplier) - amt_sell;
        }
      }
    }

    Boolean do_buy =  orders_buy  < 1 && !positiveSellOrder;
    Boolean do_sell = orders_sell < 1 && !positiveBuyOrder;

    if(do_buy || do_sell) {
      if(do_buy)
        executeSubmitOrder(buying_amt, OrderCommand.BUY);
      if(do_sell)
        executeSubmitOrder(selling_amt, OrderCommand.SELL);

    } else if(Double.compare(Math.abs(amt_buy - amt_sell), 0.0) >= 0) {
      if(!positiveBuyOrder && Double.compare(SharedProps.round(amt_buy - amt, 3), amt_sell) >= 0) {
        executeSubmitOrder(amt_buy - amt_sell, OrderCommand.SELL);
      }
      if(!positiveSellOrder && Double.compare(SharedProps.round(amt_sell - amt, 3), amt_buy) >= 0) {
        executeSubmitOrder(amt_sell - amt_buy, OrderCommand.BUY);
      }
    }
  }

  //
  private void executeSubmitOrder(double amountCurrent, OrderCommand orderCmd) {
    double lastPrice;
    try {
      if (orderCmd == OrderCommand.BUY) {
        SharedProps.print(inst_str + " submit Open-new Buy amt=" + amountCurrent);
        lastPrice = getLastTick().getAsk();
        executeSubmitOrderProceed("UP", orderCmd, amountCurrent, lastPrice);

      } else if (orderCmd == OrderCommand.SELL) {
        SharedProps.print(inst_str + " submit Open-new Sell amt=" + amountCurrent);
        lastPrice = getLastTick().getBid();
        executeSubmitOrderProceed("DW", orderCmd, amountCurrent, lastPrice);
      }
    } catch (Exception e) {
      SharedProps.print("IOrder call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
  }
  private void executeSubmitOrderProceed(String trend, OrderCommand cmd, double amount, double atPrice){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new SubmitOrderProceed(trend, cmd, amount, atPrice));
        } catch (Exception e) {
          SharedProps.print("executeSubmitOrderProceed call() E: "+e.getMessage()+" " +
              "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
        }
      }
    }).start();
    inst_exec_ts.set(SharedProps.get_sys_ts());
  }
  private class SubmitOrderProceed implements Callable<IOrder> {
    private final double amt, at;
    private final OrderCommand cmd;
    private final String trend;

    public SubmitOrderProceed(String dir, OrderCommand orderCmd, double amount, double atPrice) {
      trend = dir;
      cmd = orderCmd;
      amt = SharedProps.round(amount, 6);
      at = atPrice;
    }
    public IOrder call() {
      IOrder new_order=null;
      try {
        long ts = SharedProps.get_sys_ts();
        new_order = engine.submitOrder("O"+ts+"_"+trend, inst, cmd, amt);
        if(new_order != null && new_order.getId() != null)
          SharedProps.oBusy.put(new_order.getId(), ts + 900);

        if(configs.debug)
          SharedProps.print(inst_str+"-"+cmd.toString()+"" +" amt:"+amt+ " at:"+at);
        inst_busy_exec_ts.set(ts + 10000);

      }catch (Exception e){
        SharedProps.print("SubmitOrderProceed call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      inst_exec_ts.set(SharedProps.get_sys_ts());
      return new_order;
    }
  }

  //
  private void executeClose(IOrder order, double _amt, double _price) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new CloseProceed(order, _amt, _price));
        } catch (Exception e) {
          SharedProps.oBusy.put(order.getId(), SharedProps.get_sys_ts() - 1000);
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
      } catch (Exception e){
        SharedProps.print("CloseProceed call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +o.getInstrument());
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() - 1000);
      }
      return o;
    }
  }


  //
  private ITick getLastTick() {
    for(;;) {
      try{
        ITick lastTick = history.getLastTick(inst);
        if(lastTick != null && !Double.isNaN(lastTick.getAsk()) && !Double.isNaN(lastTick.getBid()))
          return lastTick;
      } catch (Exception e){}
      try{ Thread.sleep(2); } catch (Exception e){}
    }
  }
  private double getOffersDifference() {
    ITick lastTick = getLastTick();
    return lastTick.getAsk()-lastTick.getBid();
  }
  private double getCommisionPip(IOrder o) {
    double value = Math.abs(o.getCommission());
    double pl = Math.abs(o.getProfitLossInAccountCurrency());
    double pip = Math.abs(o.getProfitLossInPips());
    double chk = 0.5;
    double rslt = 0.0;
    double rate = 0.0;
    if(Double.compare(pl, 1.0) > 0 && Double.compare(pip, 1.0) > 0)
      chk = SharedProps.round(value/(pl/pip) + 0.1, 1);
    try {
      rate = context.getUtils().convertPipToCurrency(
        inst,
        configs.account_currency,
        (o.getOrderCommand() == OrderCommand.BUY ? OfferSide.ASK : OfferSide.BID)
      );
      if(!Double.isNaN(rate) && Double.compare(rate, 0.0) != 0) {
        rate *= o.getAmount() * 1000 * (1000); // account_currency == pip-value (0.0001)
        rslt = SharedProps.round(value/rate + 0.1, 1);
      }
    } catch (Exception e) { }
    if(configs.debug) {
      SharedProps.print(
        inst_str + " getCommisionPip rate:" + SharedProps.round(rate, 1) +
        " value:" + value + " chk:" + chk + " rslt:" + rslt
      );
    }
    return SharedProps.round((Double.compare(rslt, 0.0) == 0 ? chk: rslt) * 2.2, 1);
  }

  //
  private void setInstFirstStep(long ts) {
    try {
      double step = (getLastTick().getAsk() / (inst_pip/10)) * 0.00001;
      step = configs.trail_step_1st_min * (Double.compare(step, 1) < 0 ? (1/step) : step);
      minFirstStepByInst = Double.compare(step, configs.trail_step_1st_min) > 0 ? step : configs.trail_step_1st_min;
      min_1st_step_inst_ts = ts;
    } catch (Exception e) {
      SharedProps.print("setInstFirstStep getLastTick call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
  }

  //
  private double getFirstStep() {
    double step = minFirstStepByInst/configs.trail_step_1st_divider;
    return (Double.compare(step, configs.trail_step_1st_min) > 0 ? step : configs.trail_step_1st_min) * inst_pip;
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
      if(configs.debug)
        SharedProps.print("getMin E: "+e.getMessage()+" Thread: "+Thread.currentThread().getName()+" "+e+" "+
            inst_str+"@"+time_frame);
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
      if(configs.debug)
        SharedProps.print("getMax E: "+e.getMessage()+" Thread: "+Thread.currentThread().getName()+" "+e+" "+
            inst_str+"@"+time_frame);
    }
    return Double.NaN;
  }


  @Override
  public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) {}


  @Override
  public void onMessage(IMessage message) {
    try {
      IOrder order = message.getOrder();
      Instrument o_inst = order.getInstrument();
      if(!o_inst.toString().equals(inst_str))
        return;

      switch(message.getType()) {

        case ORDER_SUBMIT_OK:
          if(configs.debug)
            SharedProps.print("Order submit ok: " + order);
          onTick(o_inst, null);
          break;
        case ORDER_SUBMIT_REJECTED:
          if(configs.debug)
            SharedProps.print("Order submit rejected: " + order);
          inst_busy_exec_ts.set(SharedProps.get_sys_ts() + 10000);
          onTick(o_inst, null);
          break;

        case ORDER_CHANGED_OK:
          if(configs.debug)
            SharedProps.print("Order changed ok: " + order);
          onTick(o_inst, null);
          break;
        case ORDER_CHANGED_REJECTED:
          SharedProps.print("Order change rejected: " + order);
          onTick(o_inst, null);
          break;

        case ORDERS_MERGE_OK:
          if(configs.debug)
            SharedProps.print("Orders merged ok: " + order);
          orders_trailingstep.remove(order.getId());
          onTick(o_inst, null);
          break;
        case ORDERS_MERGE_REJECTED:
          if(configs.debug)
            SharedProps.print("Orders merge rejected: " + order);
          onTick(o_inst, null);
          break;

        case ORDER_FILL_OK :
          if(configs.debug)
            SharedProps.print("Order fill ok: " + order);
          inst_busy_exec_ts.set(SharedProps.get_sys_ts());
          onTick(o_inst, null);
          break;
        case ORDER_FILL_REJECTED:
          if(configs.debug)
            SharedProps.print("Order fill rejected: " + order);
          inst_busy_exec_ts.set(SharedProps.get_sys_ts() + 10000);
          onTick(o_inst, null);
          break;

        case ORDER_CLOSE_OK:
          if(configs.debug)
            SharedProps.print("Order close ok: " + order);
          orders_trailingstep.remove(order.getId());
          onTick(o_inst, null);
          break;
        case ORDER_CLOSE_REJECTED:
          if(configs.debug)
            SharedProps.print("Order close rejected: " + order);
          onTick(o_inst, null);
          break;

        case STOP_LOSS_LEVEL_CHANGED:
          if(configs.debug)
            SharedProps.print("ST changed ok: " + order);
          break;

        default:
          if(configs.debug)
            SharedProps.print("default msg-event: " + order);
          break;

      }
      onTick(o_inst, null);

    } catch (Exception e) {
    }
  }



  private double getAmount(int num_orders) {
    double amt = configs.get_amount();
    return amt;
  }

  @Override
  public void onStop()  {
    stop_run.set(true);
    if(!configs.emailReports || configs.reportEmail.isEmpty())
      return;
    try {
      context.getUtils().sendMail(
        configs.reportEmail,
        configs.reportName+" stopped",
        "strategy stopped:" + strategyId
      );
    } catch (Exception e) {
      //print("onMessage E: "+e.getMessage()+" Thread: " + Thread.currentThread().getName() + " " + e);
    }
  }

}