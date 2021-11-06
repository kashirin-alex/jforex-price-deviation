// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.*;
import com.dukascopy.api.IEngine.OrderCommand;
import com.dukascopy.api.IIndicators.AppliedPrice;
import com.dukascopy.api.system.IClient;

import java.util.*;
import java.util.concurrent.Callable;


public class PriceDeviationInstrument implements IStrategy {

  public boolean stop_run = false;
  public boolean pid_restart = false;

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
  private long inst_exec_ts;
  private boolean inst_init = false;

  private HashMap<String, Double> inst_last_price = new HashMap<>();

  private boolean inst_busy = false;
  private long inst_busy_ts = SharedProps.get_sys_ts();
  private int inst_tick_count = 0;
  private long inst_busy_exec_ts = inst_busy_ts;

  private double minFirstStepByInst;
  private long min_1st_step_inst_ts = 0;


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

    if(configs.onlyDifferentTick) {
      inst_last_price.put(inst_str+"_Ask", 0.0);
      inst_last_price.put(inst_str+"_Bid", 0.0);
    }

    minFirstStepByInst = configs.trail_step_1st_min * inst_pip;

    new Thread(new Runnable() {
      @Override
      public void run() {runInstrument();}
    }).start();
  }


  private void runInstrument() {
    int counter = 999;
    long ts = SharedProps.get_sys_ts();
    inst_exec_ts = ts + 10000;
    while(!stop_run) {
      try{
        if(!SharedProps.inst_active.get(inst_str)) {
          pid_restart = false;
          Thread.sleep(10000);
          continue;
        }
        Thread.sleep(configs.execute_inst_ms);
      }catch (Exception e){ }
  
      ts = SharedProps.get_sys_ts();

      if(ts-min_1st_step_inst_ts > 60000)
        setInstFirstStep(ts);

      if(++counter == 1000) {
        if(configs.debug)
          SharedProps.print(
            inst_str +
            " step: " + SharedProps.round(getFirstStep()/inst_pip, 1) +
            " diff: " + SharedProps.round(getOffersDifference()/inst_pip, 1)
          );
        counter = 0;
      }
      if(ts-inst_exec_ts < 10000)
        continue;
      if(inst_busy_exec_ts > ts)
        continue;

      executeInstrument(ts);
      pid_restart = false;
      inst_init = true;
    }
  }

  @Override
  public void onTick(final Instrument instrument, ITick tick) {
    if(!instrument.toString().equals(inst_str)) return;
    SharedProps.set_ts(tick.getTime());
    pid_restart = false;

    if(configs.onlyDifferentTick) {
      if(Double.compare(inst_last_price.get(inst_str+"_Ask"), tick.getAsk()) == 0
          && Double.compare(inst_last_price.get(inst_str+"_Bid"), tick.getBid()) == 0)
        return;
      inst_last_price.put(inst_str+"_Ask", tick.getAsk());
      inst_last_price.put(inst_str+"_Bid", tick.getBid());
    }

    if(inst_busy) {
      inst_tick_count++;
      long ts = SharedProps.get_sys_ts();
      if (ts - inst_busy_ts > 30000) {
        if(configs.debug)
          SharedProps.print(inst_str + " busy: " + (ts - inst_busy_ts) + " ticks behind: " + inst_tick_count);
        inst_tick_count = 0;

        if (ts - inst_busy_ts > 300000) {
          if (inst_thread != null) {
            try {
              inst_thread.interrupt();
            } catch (Exception e) {
            }
            inst_thread = null;
          }
          inst_busy = false;
        }
      }
    }

    if(inst_busy) return;
    inst_busy = true;
    inst_tick_count = 0;

    inst_thread = new Thread(new Runnable() {
      @Override
      public void run() { manageInstrument(); }
    });
    inst_thread.start();
    inst_busy_ts = SharedProps.get_sys_ts();
  }


  //
  private void manageInstrument() {
    try {

      List<IOrder> orders = new ArrayList<>();
      List<IOrder> mergeBuyOrders = new ArrayList<>();
      List<IOrder> mergeSellOrders = new ArrayList<>();

      double currentAmount;
      OrderCommand cmd;
      long ts = SharedProps.get_sys_ts();

      double trailingStepPip, lastTickBid, lastTickAsk, trailSLprice, trailingStep;
      double o_profit, o_cost, o_swap;
    
      int num_buy_orders=0;
      int num_sell_orders=0;

      for(IOrder o : engine.getOrders(inst)) {
        if(o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED) continue;
        if(o.getLabel().contains("Signal:") && !configs.manageSignals) continue;
        cmd = o.getOrderCommand();
        if(cmd == OrderCommand.BUY) {
          num_buy_orders++;
          orders.add(o);
        } else if( cmd == OrderCommand.SELL){
          num_sell_orders++;
          orders.add(o);
        }
        if (!SharedProps.oBusy.containsKey(o.getId()))
          SharedProps.oBusy.put(o.getId(), ts);
      }

      double step_1st = getFirstStep();
      double step_1st_pip = step_1st/inst_pip;

      if(inst_init && orders.size() > 1) {
        for( IOrder o : orders) {
          cmd = o.getOrderCommand();
          if (cmd == OrderCommand.BUY) {
            mergeBuyOrders.add(o);
          } else if (cmd == OrderCommand.SELL) {
            mergeSellOrders.add(o);
          }
        }
        double require_pip = step_1st_pip * configs.merge_followup_step_muliplier;
          
        if(mergeBuyOrders.size() > 1) {
          mergeBuyOrders.sort(new Comparator<IOrder>() {
            @Override
            public int compare(IOrder lhs, IOrder rhs) {
              return lhs.getProfitLossInPips() > rhs.getProfitLossInPips() ? -1 : 1;
            }
          });
          double pip_buy = 0, amt_buy = 0;
          int num = 0;
          for( IOrder o : mergeBuyOrders) {
            double amt = o.getAmount()*1000;
            amt_buy += amt;
            pip_buy += o.getProfitLossInPips() * amt;
            pip_buy -= getCommisionPip(o);
            if(++num < 2)
              continue;
            if(Double.compare((pip_buy / amt_buy) * (1-configs.trail_step_rest_plus_gain), require_pip) < 0) {
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
              return lhs.getProfitLossInPips() > rhs.getProfitLossInPips() ? -1 : 1;
            }
          });
          double pip_sell = 0, amt_sell = 0;
          int num = 0;
          for( IOrder o : mergeSellOrders) {
            double amt = o.getAmount()*1000;
            amt_sell += amt;
            pip_sell += o.getProfitLossInPips() * amt;
            pip_sell -= getCommisionPip(o);
            if(++num < 2)
              continue;
            if(Double.compare((pip_sell / amt_sell) * (1-configs.trail_step_rest_plus_gain), require_pip) < 0) {
              mergeSellOrders.clear();
            } else if(mergeSellOrders.size() > 2) {
              mergeSellOrders = mergeSellOrders.subList(0, 2);
            }
            break;
          }
        }
  
        if(mergeSellOrders.size() >= 2 || mergeBuyOrders.size() >= 2) {
          orders.clear();
        } else {
          mergeBuyOrders.clear();
          mergeSellOrders.clear();
        }
      }

  
      if(inst_init && orders.size() > 1) {
        mergeBuyOrders.addAll(orders);
        mergeBuyOrders.sort(new Comparator<IOrder>() {
          @Override
          public int compare(IOrder lhs, IOrder rhs) {
            return lhs.getProfitLossInPips() > rhs.getProfitLossInPips() ? -1 : 1;
          }
        });
        IOrder pos = mergeBuyOrders.get(0);
        IOrder neg = mergeBuyOrders.get(mergeBuyOrders.size()-1);
        mergeBuyOrders.clear();

        double sl = pos.getStopLossPrice();
        if(pos.getOrderCommand() != neg.getOrderCommand() &&
           Double.compare(pos.getTrailingStep(), 0.0) > 0 &&
           Double.compare(sl, 0.0) != 0 &&
           Double.compare(pos.getAmount(), neg.getAmount()) > 0 &&
           Double.compare(pos.getProfitLossInAccountCurrency(), neg.getProfitLossInAccountCurrency()) > 0) {
          double pos_dif, neg_dif;
          ITick t = getLastTick();
          if(pos.getOrderCommand() == OrderCommand.BUY) {
            pos_dif = sl - pos.getOpenPrice();
            neg_dif = t.getAsk() - neg.getOpenPrice();
          } else {
            pos_dif = pos.getOpenPrice() - sl;
            neg_dif = neg.getOpenPrice() - t.getBid();
          }
          pos_dif *= (pos.getAmount()*1000);
          neg_dif *= (neg.getAmount()*1000);
          pos_dif /= 1000;
          neg_dif /= 1000;
    
          if(Double.compare(pos_dif - step_1st * configs.merge_followup_step_muliplier, neg_dif) > 0) {
            SharedProps.print(
              inst_str + " CloseMergable: diff=" +
              SharedProps.round((pos_dif - step_1st * configs.merge_followup_step_muliplier) - neg_dif, inst_scale + 2) + 
              " @ " + pos.getProfitLossInAccountCurrency() + " > " + neg.getProfitLossInAccountCurrency()
            );
            executeCloseMergable(neg);
            orders.remove(neg);
          }
        }
      }


      if(inst_init && orders.size() > 1) {
        for(IOrder o : orders) {
          if(Double.compare(o.getProfitLossInPips(), 0.0) < 0) {
            cmd = o.getOrderCommand();
            if(cmd == OrderCommand.SELL)
              mergeSellOrders.add(o);
            else if(cmd == OrderCommand.BUY)
              mergeBuyOrders.add(o);
          }
        }
        if(mergeSellOrders.size() < 2)
          mergeSellOrders.clear();
        if(mergeBuyOrders.size() < 2)
          mergeBuyOrders.clear();
      }


      ////
      for( IOrder o : orders) {
          if(Double.compare(o.getProfitLossInPips(), step_1st_pip) <= 0 ||
             ts - SharedProps.oBusy.get(o.getId()) < 888) {
            continue;
          }

          cmd = o.getOrderCommand();
          trailingStep = step_1st;
          if(o.getLabel().contains("Signal:"))
            trailingStep = trailingStep*2;

          o_cost = getCommisionPip(o) + 0.5;
          if(configs.debug)
            SharedProps.print(inst_str+" o_cost:"+SharedProps.round(o_cost, 2) +
                              " step:"+SharedProps.round((trailingStep/inst_pip), 2));
          o_cost *= inst_pip;
          o_profit = o.getProfitLossInPips() * inst_pip;
          if(Double.compare(o_profit - o_cost, trailingStep * 2) <= 0)
            continue;

          if(Double.compare(o_profit - trailingStep,  o_profit * configs.trail_step_rest_plus_gain) > 0)
            trailingStep += o_profit * configs.trail_step_rest_plus_gain;

          trailingStepPip = SharedProps.round((trailingStep/inst_pip)*1.2, inst_scale);
          ITick t = getLastTick();
          lastTickBid = t.getBid();
          lastTickAsk = t.getAsk();

          if (cmd == OrderCommand.BUY) {
            trailSLprice = lastTickBid - trailingStep - (lastTickAsk - lastTickBid);
            trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);

            if (Double.compare(trailSLprice, 0) > 0 && Double.compare(o.getOpenPrice(), trailSLprice) < 0) {
              if (Double.compare(o.getStopLossPrice(), trailSLprice) < 0 || Double.compare(o.getStopLossPrice(), 0) == 0) {
                if (Double.compare(o.getTrailingStep(), 0) == 0 || Double.compare(o.getStopLossPrice(), trailSLprice) < 0)
                  executeTrailOrder(cmd, o, trailSLprice, trailingStepPip);
              }
            }

          } else if (cmd == OrderCommand.SELL) {
            trailSLprice = lastTickAsk + trailingStep + (lastTickAsk - lastTickBid);
            trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);

            if (Double.compare(trailSLprice, 0) > 0 && Double.compare(o.getOpenPrice(), trailSLprice) > 0) {
              if (Double.compare(o.getStopLossPrice(), trailSLprice) > 0 || Double.compare(o.getStopLossPrice(), 0) == 0) {
                if (Double.compare(o.getTrailingStep(), 0) == 0 || Double.compare(o.getStopLossPrice(), trailSLprice) > 0)
                  executeTrailOrder(cmd, o, trailSLprice, trailingStepPip);
              }
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

    } catch (Exception e) {

      SharedProps.print("ManageInstOrders call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }

    inst_busy = false;
    inst_thread = null;
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
  private void executeInstrument(long ts) {
    try {
  
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
      double o_profit, o_cost, o_amt;
      double step_1st_pip = getFirstStep()/inst_pip;

      for( IOrder o : engine.getOrders(inst)) {
        if(o.getLabel().contains("Signal:")) continue;
        if(o.getState() != IOrder.State.FILLED &&
           o.getState() != IOrder.State.OPENED &&
           o.getState() != IOrder.State.CREATED) continue;

        cmd = o.getOrderCommand();
        o_profit = o.getProfitLossInPips();
        o_cost = getCommisionPip(o);
        o_amt = o.getAmount();

        if (cmd == OrderCommand.BUY || cmd == OrderCommand.PLACE_BID || cmd == OrderCommand.BUYLIMIT || cmd == OrderCommand.BUYSTOP) {
          totalBuyOrder ++;
          if(cmd == OrderCommand.BUY) {
            totalBoughtAmount = totalBoughtAmount + o_amt;

            if(Double.compare(o.getTrailingStep(), 0) > 0 && Double.compare(o_profit + o_cost, 0) > 0) {
              o_amt = Double.compare(o_amt, amt) < 0 ? amt : o_amt;
              if(Double.compare(
                  (o_profit * (1 - configs.trail_step_rest_plus_gain) - o_cost) / step_1st_pip,
                  configs.open_followup_step_muliplier + (configs.open_followup_step_muliplier * ((o_amt/amt)/100)) ) > 0) {
                --totalBuyOrder;
                followUpBuyOrder = true;
              }
              positiveBuyOrder = true;
            }
          }

        }else if(cmd == OrderCommand.SELL || cmd == OrderCommand.PLACE_OFFER || cmd == OrderCommand.SELLLIMIT || cmd == OrderCommand.SELLSTOP) {
          totalSellOrder ++;
          if(cmd == OrderCommand.SELL) {
            totalSoldAmount = totalSoldAmount + o_amt;

            if(Double.compare(o.getTrailingStep(), 0) > 0 && Double.compare(o_profit + o_cost, 0) > 0) {
              o_amt = Double.compare(o_amt, amt) < 0 ? amt : o_amt;
              if(Double.compare(
                  (o_profit * (1 - configs.trail_step_rest_plus_gain) - o_cost) / step_1st_pip,
                  configs.open_followup_step_muliplier + (configs.open_followup_step_muliplier * ((o_amt/amt)/100)) ) > 0) {
                --totalSellOrder;
                followUpSellOrder = true;
              }
              positiveSellOrder = true;
            }
          }
        }
      }

      if(Double.compare(totalBoughtAmount, 0.0) > 0 && Double.compare(totalBoughtAmount, amt) < 0)
        --totalBuyOrder;
      if(Double.compare(totalSoldAmount, 0.0) > 0 && Double.compare(totalSoldAmount, amt) < 0)
        --totalSellOrder;

      Boolean do_buy =  totalBuyOrder < 1 && !positiveSellOrder;
      Boolean do_sell = totalSellOrder < 1 && !positiveBuyOrder;
  
      if(do_buy || do_sell) {
        if(do_buy)
          executeSubmitOrder(amt, OrderCommand.BUY);
        if(do_sell)
          executeSubmitOrder(amt, OrderCommand.SELL);
    
      } else if(Double.compare(Math.abs(totalBoughtAmount - totalSoldAmount), amt) >= 0) {
        if(!positiveBuyOrder &&
            Double.compare(totalBoughtAmount * configs.open_support_side_ratio - amt, totalSoldAmount) >= 0) {
          executeSubmitOrder(totalBoughtAmount * configs.open_support_side_ratio - totalSoldAmount, OrderCommand.SELL);
        }
        if(!positiveSellOrder &&
            Double.compare(totalSoldAmount * configs.open_support_side_ratio - amt, totalBoughtAmount) >= 0) {
          executeSubmitOrder(totalSoldAmount * configs.open_support_side_ratio - totalBoughtAmount, OrderCommand.BUY);
        }
      }
  
    } catch (Exception e) {
      SharedProps.print("executeInstrument E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
  }

  //
  private void executeSubmitOrder(double amountCurrent, OrderCommand orderCmd) {
    double lastPrice;
    try {
      if (orderCmd == OrderCommand.BUY) {
        lastPrice = getLastTick().getAsk();
        executeSubmitOrderProceed("UP", orderCmd, amountCurrent, lastPrice);

      } else if (orderCmd == OrderCommand.SELL) {
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
    inst_exec_ts = SharedProps.get_sys_ts();
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

        SharedProps.print(inst_str+"-"+cmd.toString()+"" +" amt:"+amt+ " at:"+at);
        inst_busy_exec_ts = ts + 10000;

      }catch (Exception e){
        SharedProps.print("SubmitOrderProceed call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      inst_exec_ts = SharedProps.get_sys_ts();
      return new_order;
    }
  }

  //
  private void executeCloseMergable(IOrder order) {
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new CloseMergable(order));
        } catch (Exception e) {
        }
      }
    }).start();
  }

  private class CloseMergable implements Callable<IOrder> {
    private final IOrder o;

    public CloseMergable(IOrder order) {
      o = order;
    }
    public IOrder call() {
      try {
        o.close();
      } catch (Exception e){
        SharedProps.print("CloseMergable call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +o.getInstrument());
      }
      return o;
    }
  }


  //
  private ITick getLastTick() {
    ITick tick;
    for(int i=0;i<3;i++){
      try{
        tick = history.getLastTick(inst);
        if(tick!=null) return tick;
        Thread.sleep(1);
      } catch (Exception e){}
    }
    return null;
  }
  private double getOffersDifference(){
    ITick t = getLastTick();
    return t.getAsk()-t.getBid();
  }
  private double getCommisionPip(IOrder o){
    return (Math.abs(o.getCommission()/(o.getProfitLossInAccountCurrency()/o.getProfitLossInPips()))) +
            (getOffersDifference()/inst_pip) + 0.2;
  }

  //
  private void setInstFirstStep(long ts) {
    try {
      double step = (getLastTick().getAsk()/(inst_pip/10))*0.00001;
      step = configs.trail_step_1st_min*(step<1?(1/step):step);
      minFirstStepByInst = Double.compare(step, configs.trail_step_1st_min)>0?step:configs.trail_step_1st_min;
      min_1st_step_inst_ts = ts;
    } catch (Exception e) {
      SharedProps.print("setInstFirstStep getLastTick call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
  }

  //
  private double getFirstStep() {
    double step = minFirstStepByInst/configs.trail_step_1st_divider;
    return (Double.compare(step, configs.trail_step_1st_min)>0?step:configs.trail_step_1st_min)*inst_pip;
  }



  @Override
  public void onBar(Instrument instrument, Period period, IBar askBar, IBar bidBar) {}

    
  @Override
  public void onMessage(IMessage message) {
    try {
      IOrder order = message.getOrder();
      if(!order.getInstrument().toString().equals(inst_str))
        return;

      switch(message.getType()) {

        case ORDER_SUBMIT_OK :
          SharedProps.print("Order submit ok: " + message.getOrder());
          break;
        case ORDER_SUBMIT_REJECTED :
          SharedProps.print("Order submit rejected: " + message.getOrder());
          inst_busy_exec_ts = SharedProps.get_sys_ts() + 10000;
          break;
  
        case ORDER_CHANGED_OK:
          SharedProps.print("Order changed ok: " + message.getOrder());
          break;
        case ORDER_CHANGED_REJECTED:
          SharedProps.print("Order change rejected: " + message.getOrder());
          break;

        case ORDERS_MERGE_OK:
          SharedProps.print("Orders merged ok: " + message.getOrder());
          break;
        case ORDERS_MERGE_REJECTED:
          SharedProps.print("Orders merge rejected: " + message.getOrder());
          break;

        case ORDER_FILL_OK :
          SharedProps.print("Order fill ok: " + message.getOrder());
          inst_busy_exec_ts = SharedProps.get_sys_ts();
          break;
        case ORDER_FILL_REJECTED :
          SharedProps.print("Order fill rejected: " + message.getOrder());
          inst_busy_exec_ts = SharedProps.get_sys_ts() + 10000;
          break;

        case STOP_LOSS_LEVEL_CHANGED :
          SharedProps.print("ST changed ok: " + message.getOrder());
          break;
  
        case ORDER_CLOSE_OK :
          SharedProps.print("Order close ok: " + message.getOrder());
          break;
        case ORDER_CLOSE_REJECTED :
          SharedProps.print("Order close rejected: " + message.getOrder());
          break;
      
        default:
          SharedProps.print("default msg-event: " + message.getOrder());
          break;
          
      }

    } catch (Exception e) {
    }
  }

  

  private double getAmount(int num_orders) {
    return configs.get_amount();
  }

  @Override
  public void onStop()  {
    stop_run = true;
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