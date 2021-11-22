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
  private double rnd_ratio = 0.0;
  private AtomicBoolean inst_init = new AtomicBoolean(false);
  
  private AtomicLong inst_exec_ts = new AtomicLong(0);
  private AtomicBoolean inst_busy = new AtomicBoolean(false);
  private AtomicLong inst_busy_ts = new AtomicLong(SharedProps.get_sys_ts());
  private AtomicLong inst_busy_exec_ts = new AtomicLong(inst_busy_ts.get());
  private AtomicInteger inst_tick_count = new AtomicInteger(0);

  private double minFirstStepByInst;
  private long min_1st_step_inst_ts = 0;
  private long inst_std_dev_ts = 0;

  private HashMap<String, Long> inst_tf_ts = new HashMap<>();
  private HashMap<String, Double> inst_tf = new HashMap<>();
  private HashMap<String, Long> inst_tf_gf = new HashMap<>();

  private double inst_timeframe = 0;
  private HashMap<String, Double> inst_timeframes = new HashMap<>();

  private double inst_pip_dist = 0;
  private HashMap<String, Double> inst_pip_dists = new HashMap<>();

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
    SharedProps.inst_std_dev_avg.put(inst_str, Double.NaN);

    new Thread(new Runnable() {
      @Override
      public void run() {runInstrument();}
    }).start();
  }


  private void runInstrument() {
    int counter = 999;
    double std_dev;
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

      if(ts-min_1st_step_inst_ts > 60000)
        setInstFirstStep(ts);

      if(ts-inst_std_dev_ts > 180000)
        setStdDev(ts);

      std_dev = getStdDevPip();
      if(Double.isNaN(std_dev))
        continue;

      setTimeFrames(ts);

      if(configs.debug && ++counter == 1000) {
        SharedProps.print(inst_str +
              "|Amt: " + getAmount(1) +
              "|1stStep: " + SharedProps.round(getFirstStep()/inst_pip, 1) +
              "|min1stStep: " + SharedProps.round(minFirstStepByInst,1) +
              "|StdDevPip: " + SharedProps.round(std_dev,1) +
              "|pip_dist: " + SharedProps.round(inst_pip_dist/inst_pip,1) +
              "|diff: " + SharedProps.round(getOffersDifference()/inst_pip, 1) +
              "|timeframe: " + inst_timeframe
        );
        counter = 0;
      }
      if(ts-inst_exec_ts.get() < 20000)
        continue;
      if(Double.isNaN(inst_timeframe) || Double.compare(inst_timeframe,0) == 0)
        continue;
      if(inst_busy_exec_ts.get() > ts)
        continue;

      try {
        executeInstrument(ts, std_dev);
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
        if(o.getOrderCommand() == OrderCommand.BUY) {// && Double.compare(amt_buy, amt_sell) > 0) {
          price = lastTick.getBid();
          diff = sl - price;
          if(Double.compare(diff, 0.0) >= 0) {
            if(Double.compare(diff, step_1st) < 0) {
              closing = true;
              //amt = amt_buy - amt_sell;
              //if(Double.compare(amt, 0.001) < 0)
                //amt = 0.001;
              //if(Double.compare(amt, o.getAmount()) > 0) {
                amt = 0.0;
                amt_buy -= o.getAmount();
              //} else {
                //amt_buy -= amt;
              //}
            } else {
              clear = true;
            }
          }
        } else if(o.getOrderCommand() == OrderCommand.SELL) {// && Double.compare(amt_sell, amt_buy) > 0) {
          price = lastTick.getAsk(); 
          diff = price - sl;
          if(Double.compare(diff, 0.0) >= 0) {
            if(Double.compare(diff, step_1st) < 0) {
              closing = true;
              //amt = amt_sell - amt_buy;
              //if(Double.compare(amt, 0.001) < 0)
                //amt = 0.001;
              //if(Double.compare(amt, o.getAmount()) > 0) {
                amt = 0.0;
                amt_sell -= o.getAmount();
              //} else {
                //amt_sell -= amt;
              //}
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
      for( IOrder o : orders) {
        cmd = o.getOrderCommand();
        if (cmd == OrderCommand.BUY) {
          mergeBuyOrders.add(o);
        } else if (cmd == OrderCommand.SELL) {
          mergeSellOrders.add(o);
        }
      }
      double require_pip = step_1st_pip * configs.merge_followup_step_muliplier;
      double buy_dist = 0.0;
      double sell_dist = 0.0;

      if(mergeBuyOrders.size() > 1) {
        mergeBuyOrders.sort(new Comparator<IOrder>() {
          @Override
          public int compare(IOrder lhs, IOrder rhs) {
            return Double.compare(lhs.getProfitLossInPips(), rhs.getProfitLossInPips()) > 0 ? -1 : 1;
          }
        });
        double pip_buy = 0, _amt_buy = 0;
        int num = 0;
        for( IOrder o : mergeBuyOrders) {
          double amt = o.getAmount()*1000;
          _amt_buy += amt;
          pip_buy += o.getProfitLossInPips() * amt;
          //pip_buy -= getCommisionPip(o);
          if(++num < 2)
            continue;
          buy_dist = (pip_buy / _amt_buy); // * (1-configs.trail_step_rest_plus_gain);
          if(Double.isNaN(buy_dist) || Double.compare(buy_dist, require_pip) < 0) {
            mergeBuyOrders.clear();
          } else if(mergeBuyOrders.size() > 2) {
            mergeBuyOrders = mergeBuyOrders.subList(0, 2);
          }
          break;
        }
      }

      if(mergeSellOrders.size() > 1) {
        mergeSellOrders.sort(new Comparator<IOrder>() {
          @Override
          public int compare(IOrder lhs, IOrder rhs) {
            return Double.compare(lhs.getProfitLossInPips(), rhs.getProfitLossInPips()) > 0 ? -1 : 1;
          }
        });
        double pip_sell = 0, _amt_sell = 0;
        int num = 0;
        for( IOrder o : mergeSellOrders) {
          double amt = o.getAmount()*1000;
          _amt_sell += amt;
          pip_sell += o.getProfitLossInPips() * amt;
          //pip_sell -= getCommisionPip(o);
          if(++num < 2)
            continue;
          sell_dist = (pip_sell / _amt_sell); // * (1-configs.trail_step_rest_plus_gain);
          if(Double.isNaN(sell_dist) || Double.compare(sell_dist, require_pip) < 0) {
            mergeSellOrders.clear();
          } else if(mergeSellOrders.size() > 2) {
            mergeSellOrders = mergeSellOrders.subList(0, 2);
          }
          break;
        }
      }

      if(mergeSellOrders.size() >= 2 || mergeBuyOrders.size() >= 2) {
        orders.clear();
        if(mergeBuyOrders.size() >= 2)
          SharedProps.print(inst_str + " Merge pos-buy dist=" + buy_dist +  " require=" + require_pip);
        if(mergeSellOrders.size() >= 2)
          SharedProps.print(inst_str + " Merge pos-sell dist=" + sell_dist + " require=" + require_pip);
      } else {
        mergeBuyOrders.clear();
        mergeSellOrders.clear();
      }
    }


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

      if(neg_count == 1 && ts - SharedProps.oBusy.get(neg.getId()) < 888)
        neg_count = 0;

      boolean merge = false;
      double sl = orders_trailingstep.getOrDefault(pos.getId(), 0.0);
      if(neg_count == 1 &&
         pos.getOrderCommand() != neg.getOrderCommand() &&
         Double.compare(sl, 0.0) > 0 &&
         Double.compare(pos.getAmount(), neg.getAmount()) > 0) {
        if(Double.compare(pos.getProfitLossInAccountCurrency(), neg.getProfitLossInAccountCurrency()) > 0) {
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

          merge = Double.compare(pos_dif - step_1st * configs.merge_close_neg_side_multiplier, neg_dif) > 0;
          if(merge || configs.debug) {
            SharedProps.print(
              inst_str + " Merge " + (merge ? "" : "nominated ") + "pos-close" +
              " pos_dif=" + pos_dif + " neg_dif=" + neg_dif + " diff=" +
              SharedProps.round((pos_dif - step_1st * configs.merge_close_neg_side_multiplier) - neg_dif, inst_scale + 2) +
              " @ " + pos.getProfitLossInAccountCurrency() + " > " + neg.getProfitLossInAccountCurrency()
            );
          }
          if(merge) {
            SharedProps.oBusy.put(neg.getId(), ts + 4000);
            executeClose(neg, 0, price);
            orders.remove(neg);
          }
        }
      }
    }

    double std_dev = getStdDevPip();
    if(inst_init.get() && !Double.isNaN(std_dev) && orders.size() > 1) {
      for(IOrder o : orders) {
        double weight = o.getAmount()/getAmount(1);
        if(Double.compare(o.getProfitLossInPips(), -std_dev/(configs.merge_distance_std_dev_divider*(configs.merge_max/weight))) <= 0) {
          cmd = o.getOrderCommand();
          if(cmd == OrderCommand.SELL)
            mergeSellOrders.add(o);
          else if(cmd == OrderCommand.BUY)
            mergeBuyOrders.add(o);
        }
      }
      if(mergeSellOrders.size() < 2) {
        mergeSellOrders.clear();
      } else {
        SharedProps.print(inst_str + " Merge neg-sell std_dev=" + std_dev);
      }
      if(mergeBuyOrders.size() < 2) {
        mergeBuyOrders.clear();
      } else {
        SharedProps.print(inst_str + " Merge neg-buy std_dev=" + std_dev);
      }
    }

    ////
    double one_amt = getAmount(1);
    for( IOrder o : orders) {
      if(Double.compare(o.getProfitLossInPips(), step_1st_pip) <= 0 ||
         ts - SharedProps.oBusy.get(o.getId()) < 888) {
        continue;
      }
      cmd = o.getOrderCommand();
      if(Double.compare(amt_buy, amt_sell) == 0 ||
         (cmd == OrderCommand.BUY
          ? (Double.compare(amt_buy - configs.trail_from_diff_amount_muliplier * one_amt, amt_sell) < 0)
          : (Double.compare(amt_sell - configs.trail_from_diff_amount_muliplier * one_amt, amt_buy) < 0)) )
        continue;

      trailingStep = step_1st;
      o_profit = o.getProfitLossInPips() * inst_pip;
      double o_cost = getCommisionPip(o);
      if(Double.compare(o_profit - o_cost * inst_pip, trailingStep * configs.trail_step_entry_multiplier) <= 0)
        continue;

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

        if (Double.compare(trailSLprice, 0) > 0 && Double.compare(o.getOpenPrice(), trailSLprice) < 0) {
          if(configs.trail_managed) {
            double sl = orders_trailingstep.getOrDefault(o.getId(), 0.0);
            update_sl = Double.compare(sl, 0.0) == 0;
            if(!update_sl)
              update_sl = Double.compare(sl, trailSLprice) < 0;
          } else {
            if (Double.compare(o.getStopLossPrice(), trailSLprice) < 0 || Double.compare(o.getStopLossPrice(), 0) == 0) {
              if (Double.compare(o.getTrailingStep(), 0) == 0 || Double.compare(o.getStopLossPrice(), trailSLprice) < 0)
                update_sl = true;
            }
          }
        }

      } else if (cmd == OrderCommand.SELL) {
        lastTickAsk = lastTick.getAsk();
        trailSLprice = lastTickAsk + trailingStep; // + (lastTickAsk - lastTickBid);
        trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);

        if (Double.compare(trailSLprice, 0) > 0 && Double.compare(o.getOpenPrice(), trailSLprice) > 0) {
          if(configs.trail_managed) {
            double sl = orders_trailingstep.getOrDefault(o.getId(), 0.0);
            update_sl = Double.compare(sl, 0.0) == 0;
            if(!update_sl)
              update_sl = Double.compare(sl, trailSLprice) > 0;
          } else {
            if (Double.compare(o.getStopLossPrice(), trailSLprice) > 0 || Double.compare(o.getStopLossPrice(), 0) == 0) {
              if (Double.compare(o.getTrailingStep(), 0) == 0 || Double.compare(o.getStopLossPrice(), trailSLprice) > 0)
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
        if(Double.compare(o.getStopLossPrice(),0) > 0){
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
        if(Double.compare(o.getStopLossPrice(),0) > 0){
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
      trail_step = Double.compare(step, 10) > 0? step : (Double.compare(step, 0)==0 ? 0: 10);
      cmd = c;
    }
    public IOrder call() {
      try {
        if (cmd == OrderCommand.BUY) {
          o.setStopLossPrice(trail_price, OfferSide.BID, trail_step);
          if (Double.compare(o.getTakeProfitPrice(), 0) != 0 && Double.compare(o.getOpenPrice(), trail_price) < 0)
            o.setTakeProfitPrice(0);

        } else if (cmd == OrderCommand.SELL) {
          o.setStopLossPrice(trail_price, OfferSide.ASK, trail_step);
          if (Double.compare(o.getTakeProfitPrice(), 0) != 0 && Double.compare(o.getOpenPrice(), trail_price) > 0)
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
  private void executeInstrument(long ts, double std_dev) throws Exception {

    double amt = getAmount(1);
    int totalBuyOrder = 0;
    int totalSellOrder = 0;

    boolean followUpBuyOrder = false;
    boolean followUpSellOrder = false;

    boolean positiveBuyOrder = false;
    boolean positiveSellOrder = false;

    double totalSoldAmount = 0;
    double totalBoughtAmount = 0;

    OrderCommand cmd;
    double o_profit, o_amt;
    double step_1st_pip = getFirstStep()/inst_pip;
    double inst_cost = 0.0;
    List<IOrder> orders = new ArrayList<>();
    for(IOrder o : engine.getOrders(inst)) {
      if(o.getState() != IOrder.State.FILLED &&
         o.getState() != IOrder.State.OPENED &&
         o.getState() != IOrder.State.CREATED) continue;
      cmd = o.getOrderCommand();
      o_amt = o.getAmount();
      inst_cost += getCommisionPip(o);
      if(cmd == OrderCommand.BUY || cmd == OrderCommand.PLACE_BID || cmd == OrderCommand.BUYLIMIT || cmd == OrderCommand.BUYSTOP) {
        ++totalBuyOrder;
        totalBoughtAmount += o_amt;
        if(cmd == OrderCommand.BUY)
          orders.add(o);
      } else if(cmd == OrderCommand.SELL || cmd == OrderCommand.PLACE_OFFER || cmd == OrderCommand.SELLLIMIT || cmd == OrderCommand.SELLSTOP) {
        ++totalSellOrder;
        totalSoldAmount += o_amt;
        if(cmd == OrderCommand.SELL)
          orders.add(o);
      }
    }
    double amt_buy = SharedProps.round(totalBoughtAmount, 3);
    double amt_sell = SharedProps.round(totalSoldAmount, 3);

    double o_diff = getOffersDifference()/inst_pip;
    inst_cost += o_diff;
    o_diff += Math.abs(step_1st_pip * configs.positive_order_step_multiplier);
    double o_cost;
    boolean op_neg = Double.compare(configs.positive_order_step_multiplier, 0) < 0;

    for(IOrder o : orders) {
      o_profit = o.getProfitLossInPips();
      o_cost = o_diff + getCommisionPip(o);
      if(op_neg)
        o_cost *= -1;
      if(Double.compare(o_profit + o_cost, 0) < 0)
        continue;

      cmd = o.getOrderCommand();
      double sl = configs.trail_managed
        ? orders_trailingstep.getOrDefault(o.getId(), 0.0)
        : o.getStopLossPrice();

      boolean flw_possible =
        configs.open_followup_check_from_positive ||
        Double.compare(amt_buy, amt_sell) == 0 ||
         (cmd == OrderCommand.BUY
          ? (Double.compare(amt_buy - configs.trail_from_diff_amount_muliplier * amt, amt_sell) < 0)
          : (Double.compare(amt_sell - configs.trail_from_diff_amount_muliplier * amt, amt_buy) < 0)) ||
        Double.compare(sl, 0) > 0;

      double reach = 0;
      double require = configs.open_followup_step_muliplier;
      if(flw_possible) {
        reach = (o_profit * (1 - configs.trail_step_rest_plus_gain) - inst_cost) / step_1st_pip;
        if(Double.compare(amt_buy, amt_sell) == 0) {
          require = configs.open_followup_step_first_muliplier;
        } else if(cmd == OrderCommand.BUY ? (Double.compare(amt_buy, amt_sell) < 0) : (Double.compare(amt_buy, amt_sell) > 0)) {
          require = configs.open_followup_step_less_muliplier;
        } else {
          require *= 1.00 + (Math.abs((amt_buy - amt_sell)/amt) * configs.open_followup_require_growth_rate);
        }
        flw_possible = Double.compare(reach, require) >= 0;
      }
      o_amt = o.getAmount();
      if(cmd == OrderCommand.BUY) {
        positiveBuyOrder = true;
        if(flw_possible) {
          SharedProps.print(inst_str +  " Reached Flw Open-new Buy " + reach + " > " + require + " o_amt=" + o_amt + " total-buy=" + totalBuyOrder);
          --totalBuyOrder;
          followUpBuyOrder = true;
        }
      } else if(cmd == OrderCommand.SELL) {
        positiveSellOrder = true;
        if(flw_possible) {
          SharedProps.print(inst_str +  " Reached Flw Open-new Sell " + reach + " > " + require + " o_amt=" + o_amt + " total-sell=" + totalSellOrder);
          --totalSellOrder;
          followUpSellOrder = true;
        }
      }
    }
    
    for(IOrder o : orders) {
      cmd = o.getOrderCommand();
      o_profit = o.getProfitLossInPips();
      o_cost = o_diff + getCommisionPip(o);
      if(Double.compare(o_profit + o_cost, 0) >= 0 ||
         (positiveSellOrder && cmd == OrderCommand.BUY) ||
          positiveBuyOrder  && cmd == OrderCommand.SELL) {
        continue;
      }
      if(cmd == OrderCommand.BUY && followUpBuyOrder) {
        if(totalBuyOrder == 1)
          SharedProps.print(inst_str +  " Reached Pos Open-new Buy under-pip=" + o_profit + o_cost);
        --totalBuyOrder;
        continue;
      }
      if(cmd == OrderCommand.SELL && followUpSellOrder) {
        if(totalSellOrder == 1)
          SharedProps.print(inst_str +  " Reached Pos Open-new Sell under-pip=" + o_profit + o_cost);
        --totalSellOrder;
        continue;
      }

      double weight = o.getAmount()/amt;
      if(Double.compare(weight, configs.merge_max) < 0) {
        double reach = - SharedProps.round(std_dev/(configs.open_new_std_dev_divider*(configs.merge_max/weight)), 1);
        if(Double.compare(o_profit, reach) <= 0) {
          cmd = o.getOrderCommand();
          if(cmd == OrderCommand.BUY) {
            if(totalBuyOrder == 1)
              SharedProps.print(inst_str +  " Reached Neg Open-new Buy " + o_profit + " <= " + reach + " weight=" + weight + " std_dev=" + std_dev);
            --totalBuyOrder;
          } else if(cmd == OrderCommand.SELL) {
            if(totalSellOrder == 1)
              SharedProps.print(inst_str +  " Reached Neg Open-new Sell " + o_profit + " <= " + reach + " weight=" + weight + " std_dev=" + std_dev);
            --totalSellOrder;
          }
        }
      }
    }

    if(!positiveSellOrder && Double.compare(totalBoughtAmount, 0.0) > 0 && Double.compare(totalBoughtAmount, amt) < 0) {
      SharedProps.print(inst_str + " Adjust Open-new Buy totalBuyOrder=" + totalBuyOrder);
      totalBuyOrder = 0;
    }
    if(!positiveBuyOrder && Double.compare(totalSoldAmount, 0.0) > 0 && Double.compare(totalSoldAmount, amt) < 0) {
      SharedProps.print(inst_str + " Adjust Open-new Sell totalSoldAmount=" + totalSoldAmount);
      totalSellOrder = 0;
    }

    Boolean do_buy =  totalBuyOrder < 1 && !positiveSellOrder && (followUpBuyOrder  || getTrend("UP"));
    Boolean do_sell = totalSellOrder < 1 && !positiveBuyOrder && (followUpSellOrder || getTrend("DW"));

    if(do_buy || do_sell) {
      if(do_buy)
        executeSubmitOrder(amt, OrderCommand.BUY);
      if(do_sell)
        executeSubmitOrder(amt, OrderCommand.SELL);

    } else if(Double.compare(Math.abs(amt_buy - amt_sell), amt * configs.open_support_amt_diff) >= 0) {
      double diff = amt * configs.open_support_amt_diff - amt;
      if(!positiveBuyOrder &&
          Double.compare(amt_buy * configs.open_support_side_ratio - amt - diff, amt_sell) >= 0) {
        executeSubmitOrder(amt_buy * configs.open_support_side_ratio - amt_sell - diff, OrderCommand.SELL);
      }
      if(!positiveSellOrder &&
          Double.compare(amt_sell * configs.open_support_side_ratio - amt - diff, amt_buy) >= 0) {
        executeSubmitOrder(amt_sell * configs.open_support_side_ratio - amt_buy - diff, OrderCommand.BUY);
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
      amt = amount;
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
        o.close(amt, price, 0.1);
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
    try { 
      double value = o.getCommission();
      double rate = context.getUtils().convertPipToCurrency(
        inst,
        configs.account_currency, 
        (o.getOrderCommand() == OrderCommand.BUY ? OfferSide.ASK : OfferSide.BID)
      ) * 10000;
      if(!Double.isNaN(rate) && Double.compare(rate, 0) != 0 && !Double.isNaN(value))
        return Math.abs(SharedProps.round(value/rate + 0.1, 1));
    } catch (Exception e) {
      //SharedProps.print("getCommisionPip E: "+e.getMessage()+" " +
      //                  "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
    return 0.5;
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
  private void setStdDev(long ts) {

    double tf = configs.period_to_minutes(configs.std_dev_period)*configs.std_dev_time;
    double max = getMax(tf, OfferSide.ASK);
    double min = getMin(tf, OfferSide.BID);
    if(Double.isNaN(max) || Double.compare(max,0) == 0 || Double.isNaN(min) || Double.compare(min,0) == 0)
      return;

    double cur_std_dev = (max-min);
    double minimal = minFirstStepByInst * configs.period_to_minutes(configs.std_dev_period) * (inst_pip/10);
    cur_std_dev = Double.compare(cur_std_dev, minimal)>0?cur_std_dev:minimal;

    if (Double.isNaN(SharedProps.inst_std_dev_avg.get(inst_str)))
      SharedProps.inst_std_dev_avg.put(inst_str, cur_std_dev);
    else
      SharedProps.inst_std_dev_avg.put(inst_str, (SharedProps.inst_std_dev_avg.get(inst_str)+cur_std_dev)/2);
    inst_std_dev_ts = ts;
  }

  //
  private double getFirstStep() {
    double step = minFirstStepByInst/configs.trail_step_1st_divider;
    return (Double.compare(step, configs.trail_step_1st_min) > 0 ? step : configs.trail_step_1st_min) * inst_pip;
  }
  private double getMinStdDev(double rnd) {
    double min = minFirstStepByInst;
    min = (Double.compare(min, configs.trail_step_1st_min) > 0 ? min : configs.trail_step_1st_min) * inst_pip;
    min *= configs.profitable_ratio_min + (configs.profitable_ratio_max - configs.profitable_ratio_min) * rnd;
    min += getOffersDifference();
    return SharedProps.round(min, inst_scale+1);
  }
  private double getStdDev() {
    return SharedProps.inst_std_dev_avg.get(inst_str);
  }
  private double getStdDevPip() {
    double std_dev = SharedProps.inst_std_dev_avg.get(inst_str);
    if(!Double.isNaN(std_dev))
      return std_dev/inst_pip;
    return Double.NaN;
  }

  //
  private void setTimeFrames(long ts) {
    try {
      rnd_ratio += configs.profitable_ratio_chg;
      rnd_ratio = SharedProps.round(rnd_ratio, 2);
      if(Double.compare(rnd_ratio, 1) > 0)
        rnd_ratio=0;

      String k_r = inst_str+rnd_ratio;
      String k = inst_str+rnd_ratio;

      if(inst_tf_gf.containsKey(k_r)) {
        if(inst_tf_gf.get(k_r)-(Calendar.getInstance(TimeZone.getTimeZone("GMT"))).getTimeInMillis() > 0) {
          inst_pip_dist = inst_pip_dists.get(k_r);
          inst_timeframe = inst_timeframes.get(k_r);
          return;
        }
        if(configs.debug)
          SharedProps.print(inst_str+" ts miss "+((inst_tf_gf.get(k_r)-
              (Calendar.getInstance(TimeZone.getTimeZone("GMT"))).getTimeInMillis())/1000)+"@"+k_r);
      }
      String tmp_k;
      double minSetStdDev = getMinStdDev(rnd_ratio);

      double high, low;
      double foundTime = 0;
      double oneMinChange = 0;
      double oneMinChangeSet = 0;
      double oneMinChangeExists = 0;

      // ON 1 SECOND
      tmp_k = "_ONE_SEC";
      if(Double.compare(oneMinChangeSet,0) == 0) {
        if (inst_tf_ts.containsKey(k+tmp_k)) {
          if( ts - inst_tf_ts.get(k+tmp_k) < 1000){
            oneMinChangeSet = inst_tf.get(k+tmp_k);
            foundTime = inst_tf.get(k+tmp_k+"_ft");
          }
        }
      }
      if(Double.compare(oneMinChangeSet,0) == 0) {
        try{
          high = getMax(16, OfferSide.ASK);
          low = getMin(16, OfferSide.BID);
          oneMinChangeExists = high-low;

        } catch (Exception e) {
          if(configs.debug)
            SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
        }
        if(!Double.isNaN(oneMinChangeExists) && Double.compare(oneMinChangeExists, minSetStdDev) >= 0) {
          oneMinChangeExists=0;
          for(int n=120; n<=960; n+=7){
            int i = n/360;
            try {
              high = getMax(i, OfferSide.ASK);
              low = getMin(i, OfferSide.BID);
              oneMinChange = high-low;

              if(!Double.isNaN(oneMinChange) && Double.compare(oneMinChange,minSetStdDev) >= 0) {
                foundTime = i;
                oneMinChangeSet = oneMinChange;
                inst_tf.put(k+tmp_k, oneMinChangeSet);
                inst_tf.put(k+tmp_k+"_ft", foundTime);
              }
            } catch (Exception e) {
              if(configs.debug)
                SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
            }
            if(Double.compare(oneMinChangeSet, 0) > 0) break;
          }
        }
      }
      ///

      // ON 10 SECONDS
      tmp_k = "_TEN_SEC";
      if(Double.compare(oneMinChangeSet,0) == 0) {
        if (inst_tf_ts.containsKey(k+tmp_k)) {
          if( ts - inst_tf_ts.get(k+tmp_k) < 7500){
            oneMinChangeSet = inst_tf.get(k+tmp_k);
            foundTime = inst_tf.get(k+tmp_k+"_ft");
          }
        }
      }
      if(Double.compare(oneMinChangeSet,0) == 0) {
        try{
          high = getMax(166, OfferSide.ASK);
          low = getMin(166, OfferSide.BID);
          oneMinChangeExists = high-low;

        } catch (Exception e) {
          if(configs.debug)
            SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
        }
        if(!Double.isNaN(oneMinChangeExists) && Double.compare(oneMinChangeExists, minSetStdDev) >= 0) {
          oneMinChangeExists=0;
          for(int i=17; i<166; i++){
            try {
              high = getMax(i, OfferSide.ASK);
              low = getMin(i, OfferSide.BID);
              oneMinChange = high-low;

              if(!Double.isNaN(oneMinChange) && Double.compare(oneMinChange,minSetStdDev) >= 0) {
                foundTime = i;
                oneMinChangeSet = oneMinChange;
                inst_tf.put(k+tmp_k, oneMinChangeSet);
                inst_tf.put(k+tmp_k+"_ft", foundTime);
              }
            } catch (Exception e) {
              if(configs.debug)
                SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
            }
            if(Double.compare(oneMinChangeSet, 0) > 0) break;
          }
        }
      }
      ///

      // ON 1 MINUTE
      tmp_k = "_ONE_MIN";
      if(Double.compare(oneMinChangeSet,0) == 0) {
        if (inst_tf_ts.containsKey(k+tmp_k)) {
          if( ts - inst_tf_ts.get(k+tmp_k) < 30000){
            oneMinChangeSet = inst_tf.get(k+tmp_k);
            foundTime = inst_tf.get(k+tmp_k+"_ft");
          }
        }
      }
      if(Double.compare(oneMinChangeSet,0) == 0) {
        try{
          high = getMax(1000, OfferSide.ASK);
          low = getMin(1000, OfferSide.BID);
          oneMinChangeExists = high-low;

        } catch (Exception e) {
          if(configs.debug)
            SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
        }
        if(!Double.isNaN(oneMinChangeExists) && Double.compare(oneMinChangeExists, minSetStdDev) >= 0) {
          oneMinChangeExists=0;
          for(int i=167; i<1000; i++){
            try {
              high = getMax(i, OfferSide.ASK);
              low = getMin(i, OfferSide.BID);
              oneMinChange = high-low;

              if(!Double.isNaN(oneMinChange) && Double.compare(oneMinChange,minSetStdDev) >= 0) {
                foundTime = i;
                oneMinChangeSet = oneMinChange;
                inst_tf.put(k+tmp_k, oneMinChangeSet);
                inst_tf.put(k+tmp_k+"_ft", foundTime);
              }
            } catch (Exception e) {
              if(configs.debug)
                SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
            }
            if(Double.compare(oneMinChangeSet, 0) > 0) break;
          }
        }
      }
      ///

      // ON 5 MINUTES
      tmp_k = "_FIVE_MIN";
      if(Double.compare(oneMinChangeSet,0) == 0) {
        if (inst_tf_ts.containsKey(k+tmp_k)) {
          if( ts - inst_tf_ts.get(k+tmp_k) < 150000){
            oneMinChangeSet = inst_tf.get(k+tmp_k);
            foundTime = inst_tf.get(k+tmp_k+"_ft");
          }
        }
      }
      if(Double.compare(oneMinChangeSet,0) == 0) {
        try{
          high = getMax(5000, OfferSide.ASK);
          low = getMin(5000, OfferSide.BID);
          oneMinChangeExists = high-low;

        } catch (Exception e) {
          if(configs.debug)
            SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
        }
        if(!Double.isNaN(oneMinChangeExists) && Double.compare(oneMinChangeExists, minSetStdDev) >= 0) {
          oneMinChangeExists=0;
          for(int i=1000; i<5000; i+=16){
            try {
              high = getMax(i, OfferSide.ASK);
              low = getMin(i, OfferSide.BID);
              oneMinChange = high-low;

              if(!Double.isNaN(oneMinChange) && Double.compare(oneMinChange,minSetStdDev) >= 0) {
                foundTime = i;
                oneMinChangeSet = oneMinChange;
                inst_tf.put(k+tmp_k, oneMinChangeSet);
                inst_tf.put(k+tmp_k+"_ft", foundTime);
              }
            } catch (Exception e) {
              if(configs.debug)
                SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
            }
            if(Double.compare(oneMinChangeSet, 0) > 0) break;
          }
        }
      }
      ///

      // ON 10 MINUTES
      tmp_k = "_TEEN_MIN";
      if(Double.compare(oneMinChangeSet,0) == 0) {
        if (inst_tf_ts.containsKey(k+tmp_k)) {
          if( ts - inst_tf_ts.get(k+tmp_k) < 300000){
            oneMinChangeSet = inst_tf.get(k+tmp_k);
            foundTime = inst_tf.get(k+tmp_k+"_ft");
          }
        }
      }
      if(Double.compare(oneMinChangeSet,0) == 0) {
        try{
          high = getMax(10000, OfferSide.ASK);
          low = getMin(10000, OfferSide.BID);
          oneMinChangeExists = high-low;

        } catch (Exception e) {
          if(configs.debug)
            SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
        }
        if(!Double.isNaN(oneMinChangeExists) && Double.compare(oneMinChangeExists, minSetStdDev) >= 0) {
          oneMinChangeExists=0;
          for(int i=5000; i<10000; i+=20){
            try {
              high = getMax(i, OfferSide.ASK);
              low = getMin(i, OfferSide.BID);
              oneMinChange = high-low;

              if(!Double.isNaN(oneMinChange) && Double.compare(oneMinChange,minSetStdDev) >= 0) {
                foundTime = i;
                oneMinChangeSet = oneMinChange;
                inst_tf.put(k+tmp_k, oneMinChangeSet);
                inst_tf.put(k+tmp_k+"_ft", foundTime);
              }
            } catch (Exception e) {
              if(configs.debug)
                SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
            }
            if(Double.compare(oneMinChangeSet, 0) > 0) break;
          }
        }
      }
      ///

      // ON 15 MINUTES
      tmp_k = "_FIFTEEN_MIN";
      if(Double.compare(oneMinChangeSet,0) == 0) {
        if (inst_tf_ts.containsKey(k+tmp_k)) {
          if( ts - inst_tf_ts.get(k+tmp_k) < 450000){
            oneMinChangeSet = inst_tf.get(k+tmp_k);
            foundTime = inst_tf.get(k+tmp_k+"_ft");
          }
        }
      }
      if(Double.compare(oneMinChangeSet,0) == 0) {
        try{
          high = getMax(15000, OfferSide.ASK);
          low = getMin(15000, OfferSide.BID);
          oneMinChangeExists = high-low;

        } catch (Exception e) {
          if(configs.debug)
            SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
        }
        if(!Double.isNaN(oneMinChangeExists) && Double.compare(oneMinChangeExists, minSetStdDev) >= 0) {
          oneMinChangeExists=0;
          for(int i=10000; i<15000; i+=20){
            try {
              high = getMax(i, OfferSide.ASK);
              low = getMin(i, OfferSide.BID);
              oneMinChange = high-low;

              if(!Double.isNaN(oneMinChange) && Double.compare(oneMinChange,minSetStdDev) >= 0) {
                foundTime = i;
                oneMinChangeSet = oneMinChange;
                inst_tf.put(k+tmp_k, oneMinChangeSet);
                inst_tf.put(k+tmp_k+"_ft", foundTime);
              }
            } catch (Exception e) {
              if(configs.debug)
                SharedProps.print("getTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
            }
            if(Double.compare(oneMinChangeSet, 0) > 0) break;
          }
        }
      }
      ///

      if(!Double.isNaN(oneMinChangeSet) && Double.compare(oneMinChangeSet,0) > 0) {
        double yE = getStdDev()/oneMinChangeSet;
        double xE = ((configs.std_dev_time*configs.period_to_minutes(configs.std_dev_period))/foundTime);

        double timeframe = SharedProps.round(foundTime*(1+yE/xE), 1);
        if(Double.compare(timeframe,0) > 0) {
          inst_tf_ts.put(k, ts);

          double pips_dist = getPipsDistance(timeframe, minSetStdDev);
          if(Double.isNaN(pips_dist)) return;
          //pips_dist *= (1+(yE/xE)/2);

          if(configs.debug)
            SharedProps.print(inst_str+" tf"+
                " ft: "+foundTime+
                " xA: "+SharedProps.round(xE,1)+
                " yA: "+SharedProps.round(yE,1)+
                " r: "+SharedProps.round(1+yE/xE,2)+
                " = "+SharedProps.round(timeframe, 1)+
                " @ "+SharedProps.round(pips_dist/inst_pip, 1)+
                " >> "+SharedProps.round(minSetStdDev/inst_pip,1 )+"@"+rnd_ratio);

          Period p = configs.minutes_to_period(foundTime);
          long good_for = history.getBarStart(p, getLastTick().getTime())+
              (p.getInterval()*configs.profitable_ratio_good_for_bars);
          long cal_ts = (Calendar.getInstance(TimeZone.getTimeZone("GMT"))).getTimeInMillis();
          if(good_for < cal_ts)
            good_for += (cal_ts-good_for)+p.getInterval();
          inst_tf_gf.put(k_r, good_for);

          inst_pip_dist = SharedProps.round(pips_dist, inst_scale+1);
          inst_pip_dists.put(k_r, inst_pip_dist);

          // foundTime --  chk on smaller time
          inst_timeframe = timeframe;
          inst_timeframes.put(k_r, inst_timeframe);
          if(configs.debug) {
            SharedProps.print(inst_str +
                " ts good " + p.toString() +
                " set:"+inst_tf_gf.get(k_r)+
                " cur:"+cal_ts+
                " for:"+(((double) (inst_tf_gf.get(k_r)-cal_ts))/1000)+
                " @" + k_r);
          }

        }
      }
    } catch (Exception e) {
      if(configs.debug)
        SharedProps.print("setTimeFrames E: "+e.getMessage()+" " + e +" " +inst_str);
    }
  }

  //
  private double getPipsDistance(double tf, double min_std_dev) {
    double min = getMin(tf, OfferSide.BID);
    double max = getMax(tf, OfferSide.ASK);
    if(Double.isNaN(min) || Double.isNaN(max)) return Double.NaN;

    double pips = max-min;
    if(Double.compare(pips, min_std_dev) < 0) return Double.NaN;
    if(configs.debug)
      SharedProps.print(inst_str+" pip_dist: "+ pips+"@"+tf);
    return pips;
  }

  //
  private boolean getTrend(String trend) {
    return getPriceDifference(trend);
  }


  private boolean getPriceDifference(String trend) {
    if(Double.compare(inst_pip_dist, 0) == 0)return false;

    try{
      double price_dif = 0.0;
      double base;
      double cur;
      switch (trend){
        case "DW":
          base = getMin(inst_timeframe, OfferSide.ASK);
          if(Double.isNaN(base)) return false;
          cur = getLastTick().getBid();
          if(Double.isNaN(cur)) return false;
          price_dif = SharedProps.round(cur-base, inst_scale+1);
          if(configs.debug)
            SharedProps.print("getPriceDifference: DW "+inst_str+" "+cur+"-"+base+"="+price_dif+" @"+inst_timeframe);
          break;

        case "UP":
          base = getMax(inst_timeframe, OfferSide.BID);
          if(Double.isNaN(base)) return false;
          cur = getLastTick().getAsk();
          if(Double.isNaN(cur)) return false;
          price_dif = SharedProps.round(base-cur, inst_scale+1);
          if(configs.debug)
            SharedProps.print("getPriceDifference: UP "+inst_str+" "+base+"-"+cur+"="+price_dif+" @"+inst_timeframe);
          break;
      }

      //price_dif -= Math.abs(getOffersDifference());
      double pipDistanceSet = SharedProps.round(inst_pip_dist*configs.price_diff_multiplier, inst_scale+1);

      if(configs.debug)
        SharedProps.print("getPriceDifference: "+inst_str+"_"+trend +
          " "+SharedProps.round(price_dif - pipDistanceSet, inst_scale+1) +
          " "+SharedProps.round(price_dif, inst_scale+1) +
          ">" + SharedProps.round(pipDistanceSet, inst_scale+1) +" @"+inst_timeframe);

      if(Double.compare(price_dif, pipDistanceSet) >= 0) {
        /*
        inst_trend_info.put(inst_str+"_"+trend,
          inst_trend_info.get(inst_str+"_"+trend)+
            " "+price_dif+">"+pipDistanceSet +
            " "+configs.minutes_to_period(inst_timeframe).toString()+
            ":"+configs.minutes_to_period_scale((inst_timeframe))+
            " / "+configs.minutes_to_period(inst_timeframe,200).toString()+
              ":"+configs.minutes_to_period_scale(inst_timeframe,200));
        */
        return true;
       }

    } catch (Exception e) {
      if(configs.debug)
        SharedProps.print("getPriceDifference E: "+e.getMessage()+
                          " Thread: "+Thread.currentThread().getName()+" "+e+" "+inst_str);
    }
    return false;
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
    /*
    if(num_orders == 1)
      num_orders = 1;
    amt *= (configs.merge_max * num_orders) / ((configs.merge_max * num_orders) + 2);
    if(Double.compare(amt, 0.001) < 0)
      amt = 0.001;
    return amt;
    */
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