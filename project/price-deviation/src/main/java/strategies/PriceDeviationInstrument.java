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
  private double rnd_ratio = 0.0;
  private boolean inst_init = false;

  private HashMap<String, String> inst_trend_info = new HashMap<>();
  private HashMap<String, Double> inst_last_price = new HashMap<>();

  private boolean inst_busy = false;
  private long inst_busy_ts = SharedProps.get_sys_ts();
  private int inst_tick_count = 0;

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


  @Override
  public void onAccount(IAccount account) {}

  @Override
  public void onStart(IContext ctx) {

    SharedProps.print("onStart start");
    configs = SharedProps.configs;

    context = ctx;
    engine = ctx.getEngine();
    history = ctx.getHistory();
    indicators = ctx.getIndicators();
    inst_str = inst.toString();
    inst_pip = inst.getPipValue();
    inst_scale = inst.getPipScale();

    inst_cur_1 = inst.getPrimaryJFCurrency().getCurrencyCode();
    inst_cur_2 = inst.getSecondaryJFCurrency().getCurrencyCode();

    context.setSubscribedInstruments(java.util.Collections.singleton(inst), true);

    // init instruments data
    inst_trend_info.put(inst_str+"_DW","");
    inst_trend_info.put(inst_str+"_UP","");
    
    SharedProps.curr_a_ts.put(inst_cur_1, 0l);
    SharedProps.curr_a_ts.put(inst_cur_2, 0l);

    if(configs.onlyDifferentTick) {
      inst_last_price.put(inst_str+"_Ask", 0.0);
      inst_last_price.put(inst_str+"_Bid", 0.0);
    }

    minFirstStepByInst = configs.tp_step_1st_min*inst_pip;
    SharedProps.inst_std_dev_avg.put(inst_str, Double.NaN);

    new Thread(new Runnable() {
      @Override
      public void run() {runInstrument();}
    }).start();
  }


  private void runInstrument(){
    int counter = 0;
    double std_dev;
    long ts = SharedProps.get_sys_ts();
    inst_exec_ts = ts+120000;
    while(!stop_run) {
      try{
        if(!SharedProps.inst_active.get(inst_str)) {
          pid_restart = false;
          Thread.sleep(10000);
          continue;
        }
        Thread.sleep((long)(configs.profitable_ratio_chk_ms/(1/configs.profitable_ratio_chg))-1);
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

      counter++;
      if(counter==1000) {
        int num_buy_orders=0;
        int num_sell_orders=0;
        try{
          OrderCommand cmd;
          for( IOrder o : engine.getOrders(inst)) {
            if(o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED) continue;
            cmd = o.getOrderCommand();
            if(cmd == OrderCommand.BUY) num_buy_orders++;
            else if( cmd == OrderCommand.SELL) num_sell_orders++;
            if(configs.debug)
              SharedProps.print(inst_str +" Commission:" +o.getCommission());
          }}
        catch (Exception e){}
    
        SharedProps.print(inst_str +
              "|Amt: " + configs.get_amount() + "@" + getAmtRatioByAccCurrency() +
              "|BuyAmt: " + SharedProps.round(getAmount(num_buy_orders+1),6) +"@" + num_buy_orders +
              "|SellAmt: " + SharedProps.round(getAmount(num_sell_orders+1),6) +"@" + num_sell_orders +
              "|1stStep: " + SharedProps.round(getFirstStep()/inst_pip, 1) +
              "|min1stStep: " + SharedProps.round(minFirstStepByInst,1) +
              "|StdDevPip: " + SharedProps.round(std_dev,1) +
              "|pip_dist: " + SharedProps.round(inst_pip_dist/inst_pip,1) +
              "|timeframe: " + inst_timeframe +
              "|interval: " + ((long)(configs.profitable_ratio_chk_ms/(1/configs.profitable_ratio_chg))-1)
        );
        counter = 0;
      }
      if(ts-inst_exec_ts < 20000)
        continue;
      if(Double.isNaN(inst_timeframe) || Double.compare(inst_timeframe,0) == 0)
        continue;

      if(SharedProps.curr_a_ts.get(inst_cur_1)-ts > 0 || SharedProps.curr_a_ts.get(inst_cur_2)-ts > 0)
        continue;

      executeInstrument(ts, std_dev);
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
        if (configs.debug)
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
      List<IOrder> buyOrders = new ArrayList<>();
      List<IOrder> sellOrders = new ArrayList<>();
      List<IOrder> buyPosOrders = new ArrayList<>();
      List<IOrder> sellPosOrders = new ArrayList<>();
      List<IOrder> buyNegOrders = new ArrayList<>();
      List<IOrder> sellNegOrders = new ArrayList<>();

      double currentAmount;
      OrderCommand cmd;
      long ts = SharedProps.get_sys_ts();

      if(configs.tp_trail) {

        double trailingStepPip, trailingRestSteps, lastTickBid, lastTickAsk, trailSLprice, trailingStep;
        double o_profit, o_cost, o_swap;
    
        int num_buy_orders=0;
        int num_sell_orders=0;
        double amt_buy_orders=0;
        double amt_sell_orders=0;

        for( IOrder o : engine.getOrders(inst)) {
          if(o.getState() != IOrder.State.FILLED && o.getState() != IOrder.State.OPENED) continue;
          cmd = o.getOrderCommand();
          if(cmd == OrderCommand.BUY) {
            if(o.getLabel().contains("Signal:") && !configs.manageSignals) continue;
            num_buy_orders++;
            amt_buy_orders += o.getAmount();
            orders.add(o);
          }else if( cmd == OrderCommand.SELL){
            if(o.getLabel().contains("Signal:") && !configs.manageSignals) continue;
            num_sell_orders++;
            amt_sell_orders += o.getAmount();
            orders.add(o);
          }
          if (!SharedProps.oBusy.containsKey(o.getId()))
            SharedProps.oBusy.put(o.getId(), ts);
        }
        double step_1st = getFirstStep();
        double std_dev = getStdDevPip();
        if(inst_init && !Double.isNaN(std_dev)) {
          for( IOrder o : orders) {
            cmd = o.getOrderCommand();

            currentAmount = getAmount(cmd == OrderCommand.BUY ? num_buy_orders : num_sell_orders);
            if(Double.compare(o.getAmount()/currentAmount, configs.merge_max) >= 0) continue;

            o_profit = o.getProfitLossInPips();
            if(Double.compare(o_profit, -std_dev/configs.merge_distance_std_dev_divider) <= 0) {
              if (cmd == OrderCommand.SELL)
                sellNegOrders.add(o);
              else if (cmd == OrderCommand.BUY)
                buyNegOrders.add(o);
            }else if(Double.compare(o_profit, std_dev/configs.open_followup_std_dev_divider) > 0 // merge_positive
                && Double.compare(o.getTrailingStep(), 0) != 0) {
              if (cmd == OrderCommand.SELL)
                sellPosOrders.add(o);
              else if (cmd == OrderCommand.BUY)
                buyPosOrders.add(o);
            } else if(Double.compare(
                (cmd == OrderCommand.BUY ? amt_buy_orders : amt_sell_orders), currentAmount) < 0
                && Double.compare(o_profit, -std_dev/configs.merge_to_balance_std_dev_divider) < 0) {
              if (cmd == OrderCommand.SELL)
                sellOrders.add(o);
              else if (cmd == OrderCommand.BUY)
                buyOrders.add(o);
            }
            if(sellNegOrders.size()>=2) sellOrders = sellNegOrders;
            else if(sellPosOrders.size()>=2) sellOrders = sellPosOrders;
            if(buyNegOrders.size()>=2) buyOrders = buyNegOrders;
            else if(buyPosOrders.size()>=2) buyOrders = buyPosOrders;
          }
        }

        for( IOrder o : orders) {
          if(Double.compare(o.getProfitLossInPips() * inst_pip, step_1st) <= 0 ||
             ts - SharedProps.oBusy.get(o.getId()) < 888) {
            continue;
          }

          cmd = o.getOrderCommand();
          trailingStep = step_1st;
          if(o.getLabel().contains("Signal:"))
            trailingStep = trailingStep*2;

          o_cost = 0;

          lastTickBid = getLastTick().getBid();
          lastTickAsk = getLastTick().getAsk();

          o_swap = o_parse_comment(o.getComment());
          if(Double.compare(o_swap, 0)>0) {
            o_swap = o_swap-inst_pip_cost(cmd);
            if(Double.compare(o_swap, 0)<0) {
              if(inst_cur_2.equals(configs.account_currency.getCurrencyCode()))
                o_cost += o_swap;
              else
                o_cost += o_swap*((((lastTickBid+lastTickAsk)/2)/(inst_pip/10))*0.00001);
            }
          }
          o_swap = (SharedProps.get_ts()-o.getFillTime())/86400000;
          if(Double.compare(o_swap, 0)>0)
            o_cost += o_swap; //o_swap*trailingStep;

          o_profit = o.getProfitLossInPips();
          o_cost += (o.getCommission()/(o.getProfitLossInAccountCurrency()/o_profit));
          o_cost /= (o.getAmount()*1000);
          o_cost += (2.2+(trailingStep/inst_pip)/4);
          if(configs.debug)
            SharedProps.print(inst_str+" o_cost:"+SharedProps.round(o_cost, 2) +
                              " step:"+SharedProps.round((trailingStep/inst_pip), 2));
          o_cost *= inst_pip;
          // o_cost *= configs.o_cost_1st_step_multiplier;
          o_profit *= inst_pip;
          if(Double.compare(o_profit-o_cost, trailingStep) <= 0 && !configs.sl_trail){
            if(Double.compare(-(Math.abs(o_profit)+o_cost), -trailingStep*5) < 0)
              setTpAndSl(o);
            continue;
          }


          trailingRestSteps = trailingStep+o_profit*configs.tp_step_rest_1st_plus_gain;

          if (Double.compare(o_profit, trailingStep) >= 0
              && Double.compare(o_profit, trailingRestSteps) >= 0
              && Double.compare(o.getTrailingStep(), 0) != 0)
            trailingStep = trailingRestSteps;
      
          trailingStepPip = SharedProps.round((trailingRestSteps/inst_pip)*1.2, inst_scale);


          if (cmd == OrderCommand.BUY && buyOrders.size() < 2) {
            trailSLprice = lastTickBid - trailingStep - (lastTickAsk - lastTickBid);
            trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);

            if (Double.compare(trailSLprice, 0) > 0
                && (Double.compare(o.getOpenPrice(), trailSLprice) < 0
                || configs.sl_trail)) {

              if (Double.compare(o.getStopLossPrice(), trailSLprice) < 0
                  || Double.compare(o.getStopLossPrice(), 0) == 0) {
                if (Double.compare(o.getTrailingStep(), 0) == 0
                    || Double.compare(o.getStopLossPrice(), trailSLprice) < 0)
                  executeTrailOrder(cmd, o, trailSLprice, trailingStepPip);
              }
            }

          } else if (cmd == OrderCommand.SELL && sellOrders.size() < 2) {
            trailSLprice = lastTickAsk + trailingStep + (lastTickAsk - lastTickBid);
            trailSLprice = SharedProps.round(trailSLprice, inst_scale + 1);

            if (Double.compare(trailSLprice, 0) > 0
                && (Double.compare(o.getOpenPrice(), trailSLprice) > 0
                || configs.sl_trail)) {

              if (Double.compare(o.getStopLossPrice(), trailSLprice) > 0
                  || Double.compare(o.getStopLossPrice(), 0) == 0) {
                if (Double.compare(o.getTrailingStep(), 0) == 0
                    || Double.compare(o.getStopLossPrice(), trailSLprice) > 0)
                  executeTrailOrder(cmd, o, trailSLprice, trailingStepPip);
              }
            }
          }

        }
      }



      ////
      boolean merge = true;
      if(buyOrders.size() >= 2) {
        for(IOrder o: buyOrders){
          if(ts < SharedProps.oBusy.get(o.getId())){
            merge = false;
            break;
          }
          if(Double.compare(o.getTakeProfitPrice(),0) > 0){
            executeOrderSetTP(o, 0);
            SharedProps.oBusy.put(o.getId(), ts+10000);
            merge = false;
          }
          if(Double.compare(o.getStopLossPrice(),0) > 0){
            executeOrderSetSL(o, 0);
            SharedProps.oBusy.put(o.getId(), ts+10000);
            merge = false;
          }
        }
        if(merge) executeMergeOrders(buyOrders);
      }

      merge = true;
      if(sellOrders.size() >= 2) {
        for(IOrder o: sellOrders){
          if(ts < SharedProps.oBusy.get(o.getId())){
            merge = false;
            break;
          }
          if(Double.compare(o.getTakeProfitPrice(),0) > 0){
            executeOrderSetTP(o, 0);
            SharedProps.oBusy.put(o.getId(), ts+10000);
            merge = false;
          }
          if(Double.compare(o.getStopLossPrice(),0) > 0){
            executeOrderSetSL(o, 0);
            SharedProps.oBusy.put(o.getId(), ts+10000);
            merge = false;
          }
        }
        if(merge) executeMergeOrders(sellOrders);
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
        SharedProps.oBusy.put(o.getId(), SharedProps.get_sys_ts() + 1000);
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
        double cost_price = 0;
        double t_amt = 0;
        for(int i=0;i<=1;i++) {
          cost_price += o_parse_comment(orders.get(i).getComment())*(orders.get(i).getAmount()*1000);
          t_amt += orders.get(i).getAmount()*1000;
        }
        cost_price /= t_amt;
    
        om = engine.mergeOrders(orders.get(0).getOrderCommand().toString()+SharedProps.get_sys_ts(),  
                    o_comment(cost_price), orders.get(0), orders.get(1));
        SharedProps.oBusy.put(om.getId(), SharedProps.get_sys_ts()+2000);

      }catch (Exception e){
        SharedProps.print("MergeOrders call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      return om;
    }
  }

  //
  private void setTpAndSl(IOrder o) {
    if(!configs.tp_set && !configs.sl_set) return;
    try {

      if(SharedProps.get_sys_ts()-SharedProps.oBusy.get(o.getId()) < 240000)
        return;
      if(Double.compare(o.getTrailingStep(), 0) > 0)
        return;
      if(Double.compare(o.getStopLossPrice(),0) > 0 && !configs.tp_set && configs.sl_set)
        return;
      if(Double.compare(o.getTakeProfitPrice(),0) > 0 && !configs.sl_set && configs.tp_set)
        return;
  
      double slPrice, tpPrice, tp_pipTMP, sl_pipTMP;
      boolean busy = false;
      if (o.getOrderCommand() == OrderCommand.BUY) {
        if(configs.tp_set && Double.compare(o.getStopLossPrice(),o.getOpenPrice()) < 0) {
          tp_pipTMP = getStdDev()/configs.tp_std_dev_divider;
          if(Double.compare(tp_pipTMP, configs.tp_pip*inst_pip) <= 0)  tp_pipTMP = configs.tp_pip*inst_pip;

          tpPrice = o.getOpenPrice() + tp_pipTMP;
          tpPrice = SharedProps.round(tpPrice, inst_scale + 1);
          executeOrderSetTP(o, tpPrice);
          busy = true;
        }
        if(configs.sl_set) {
          sl_pipTMP = getStdDev()/configs.sl_std_dev_divider;
          if(Double.compare(sl_pipTMP, configs.sl_pip*inst_pip) <= 0) sl_pipTMP = configs.sl_pip/inst_pip;

          slPrice = o.getOpenPrice() - sl_pipTMP;
          slPrice = SharedProps.round(slPrice, inst_scale + 1);
          executeOrderSetSL(o, slPrice);
          busy = true;
        }
        //console.getInfo().format("%s at:%s sl:%s tp:%s LT:%s", o.getOrderCommand().toString(), lastPrice, slPrice, tpPrice, getLastTick(inst).getBid()).println();

      } else if (o.getOrderCommand() == OrderCommand.SELL) {

        if(configs.tp_set && Double.compare(o.getStopLossPrice(),o.getOpenPrice()) > 0) {
          tp_pipTMP = getStdDev()/configs.tp_std_dev_divider;
          if(Double.compare(tp_pipTMP, configs.tp_pip*inst_pip) <= 0)  tp_pipTMP = configs.tp_pip*inst_pip;

          tpPrice = o.getOpenPrice() - tp_pipTMP;
          tpPrice = SharedProps.round(tpPrice, inst_scale + 1);
          executeOrderSetTP(o, tpPrice);
          busy = true;
        }
        if(configs.sl_set) {
          sl_pipTMP = getStdDev()/configs.sl_std_dev_divider;
          if(Double.compare(sl_pipTMP, configs.sl_pip*inst_pip) <= 0) sl_pipTMP = configs.sl_pip*inst_pip;

          slPrice = o.getOpenPrice() + sl_pipTMP;
          slPrice = SharedProps.round(slPrice, inst_scale + 1);
          executeOrderSetSL(o, slPrice);
          busy = true;
        }
        //console.getInfo().format("%s at:%s sl:%s tp:%s LT:%s", o.getOrderCommand().toString(), lastPrice, slPrice, tpPrice, getLastTick(inst).getBid()).println();
      }
      if(busy)
        SharedProps.oBusy.put(o.getId(),SharedProps.get_sys_ts()+2000);
    } catch (Exception e) {
      SharedProps.print("setTpAndSl call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
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
      }catch (Exception e){
        SharedProps.print("OrderSetSL call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      return o;
    }
  }

  //
  private void executeInstrument(long ts, double std_dev) {
    try{
      if(configs.onlyOneOrder && engine.getOrders().size() > 0)return;

      int totalBuyOrder = 0;
      int totalSellOrder = 0;
      int buyOrderCount = 0;
      int sellOrderCount = 0;

      boolean newBuyOrder = true;
      boolean newSellOrder = true;
      boolean followUpBuyOrder = false;
      boolean followUpSellOrder = false;
      boolean positiveBuyOrder = false;
      boolean positiveSellOrder = false;

      double totalSoldAmount=0;
      double totalBoughtAmount=0;
      OrderCommand cmd;
      double o_profit, o_cost;

      for( IOrder o : engine.getOrders(inst)) {
        if(o.getLabel().contains("Signal:")) continue;
        if(o.getState() != IOrder.State.FILLED &&
            o.getState() != IOrder.State.OPENED &&
            o.getState() != IOrder.State.CREATED) continue;

        cmd = o.getOrderCommand();
        o_profit = o.getProfitLossInPips();
        o_cost = minFirstStepByInst+(getOffersDifference()/inst_pip);

        if (cmd == OrderCommand.BUY || cmd == OrderCommand.PLACE_BID
            || cmd == OrderCommand.BUYLIMIT || cmd == OrderCommand.BUYSTOP) {
          totalBuyOrder ++;
          if(cmd == OrderCommand.PLACE_BID) { totalBuyOrder ++; newBuyOrder = false;}
          if (cmd == OrderCommand.BUY) {
            buyOrderCount++;

            totalBoughtAmount = totalBoughtAmount + o.getAmount();

            if(Double.compare(std_dev/configs.open_new_std_dev_divider, Math.abs(o_profit)) > 0 )
              newBuyOrder = false;

            if(Double.compare(o_profit+o_cost, 0) > 0) {
              if(Double.compare(o.getTrailingStep(), 0) > 0
                && Double.compare(o_profit-o_cost, std_dev/configs.open_followup_std_dev_divider) > 0) {
                totalBuyOrder --;
                buyOrderCount --;
                newBuyOrder = true;
                followUpBuyOrder = true;
              }
              if(!configs.open_new_for_other_side) positiveBuyOrder = true;
            }
          }
          if(ts-o.getFillTime() < 10000) { newBuyOrder = false;}

        }else if(cmd == OrderCommand.SELL || cmd == OrderCommand.PLACE_OFFER
            || cmd == OrderCommand.SELLLIMIT || cmd == OrderCommand.SELLSTOP) {
          totalSellOrder ++;
          if(cmd == OrderCommand.PLACE_OFFER) {  totalSellOrder ++; newSellOrder = false;}
          if(cmd == OrderCommand.SELL) {
            sellOrderCount++;

            totalSoldAmount = totalSoldAmount + o.getAmount();

            if(Double.compare(std_dev/configs.open_new_std_dev_divider, Math.abs(o_profit)) > 0)
              newSellOrder = false;

            if(Double.compare(o_profit+o_cost, 0) > 0) {
              if(Double.compare(o.getTrailingStep(), 0) > 0
                && Double.compare(o_profit-o_cost, std_dev/configs.open_followup_std_dev_divider) > 0) {
                totalSellOrder --;
                sellOrderCount --;
                newSellOrder = true;
                followUpSellOrder = true;
              }
              if(!configs.open_new_for_other_side) positiveSellOrder = true;
            }
          }
          if(ts-o.getFillTime() < 10000) { newSellOrder = false;}
        }
      }

      double currentBuyAmount = getAmount(buyOrderCount+1);
      double currentSellAmount = getAmount(sellOrderCount+1);

      double adjustableBuyAmount = currentBuyAmount;
      double adjustableSellAmount = currentSellAmount;

      if(configs.amount_start_small) {
        if(buyOrderCount > 0)
          adjustableBuyAmount = (buyOrderCount-1)*(currentBuyAmount/buyOrderCount);
        if(sellOrderCount > 0)
          adjustableSellAmount = (sellOrderCount-1)*(currentSellAmount/sellOrderCount);
      }
      if(Double.compare(adjustableBuyAmount, totalBoughtAmount) > 0){
        currentBuyAmount= adjustableBuyAmount-totalBoughtAmount;
        totalBuyOrder = 0;
        newBuyOrder = true;
      }
      if(Double.compare(adjustableSellAmount, totalSoldAmount) > 0){
        currentSellAmount= adjustableSellAmount-totalSoldAmount;
        totalSellOrder = 0;
        newSellOrder = true;
      }

      if(configs.amount_balanced_set && SharedProps.amount_balanced_by_margin_reached) {
        if(Double.compare(totalSoldAmount,totalBoughtAmount) > 0) {
          if(!followUpBuyOrder) // buyOrderCount > 0 &&
            currentBuyAmount = totalSoldAmount-totalBoughtAmount;
          //totalBuyOrder =0;
          //newBuyOrder = true;
        }
        if(Double.compare(totalBoughtAmount,totalSoldAmount) > 0) {
          if(!followUpSellOrder) // sellOrderCount > 0 &&
            currentSellAmount = totalBoughtAmount-totalSoldAmount;
          //totalSellOrder =0;
          //newSellOrder = true;
        }
      }

      if (Double.compare(currentBuyAmount, 0.001) >= 0
          && totalBuyOrder < configs.num_orders_an_inst+1
          && configs.isBuyOrder && newBuyOrder && !positiveSellOrder) { //
        if(getTrend("UP"))
          executeSubmitOrder(currentBuyAmount, OrderCommand.BUY);
      }
      if (Double.compare(currentSellAmount, 0.001) >= 0
          && totalSellOrder < configs.num_orders_an_inst+1
          && configs.isSellOrder && newSellOrder  && !positiveBuyOrder) { //
        if(getTrend("DW"))
          executeSubmitOrder(currentSellAmount, OrderCommand.SELL);
      }

    } catch (Exception e) {
      SharedProps.print("executeInstrument E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
  }
  private void executeSubmitOrder(double amountCurrent, OrderCommand orderCmd){

    try{
      double slPrice, tpPrice, at, tmpPips, lastPrice;

      if (orderCmd == OrderCommand.BUY) {
        lastPrice = getLastTick().getAsk();

        tpPrice = 0;
        if(configs.tp_set) {
          tmpPips = getStdDev()/configs.tp_std_dev_divider;
          if(Double.compare(tmpPips, configs.tp_pip*inst_pip) <=  0)  tmpPips = configs.tp_pip*inst_pip;
          tpPrice = lastPrice + tmpPips;
          tpPrice = SharedProps.round(tpPrice, inst_scale + 1);
        }

        slPrice = 0;
        if(configs.sl_set) {
          tmpPips = getStdDev()/configs.sl_std_dev_divider;
          if(Double.compare(tmpPips, configs.sl_pip*inst_pip) <=  0)  tmpPips = configs.sl_pip*inst_pip;
          slPrice = lastPrice - tmpPips;
          slPrice = SharedProps.round(slPrice, inst_scale + 1);
        }

        at = lastPrice;
        executeSubmitOrderProceed("UP", orderCmd, amountCurrent, at, tpPrice, slPrice);

      } else if (orderCmd == OrderCommand.SELL) {
        lastPrice = getLastTick().getBid();

        tpPrice = 0;
        if(configs.tp_set) {
          tmpPips = getStdDev()/configs.tp_std_dev_divider;
          if(Double.compare(tmpPips, configs.tp_pip*inst_pip) <=  0)  tmpPips = configs.tp_pip*inst_pip;
          tpPrice = lastPrice - tmpPips;
          tpPrice = SharedProps.round(tpPrice, inst_scale + 1);
        }

        slPrice = 0;
        if(configs.sl_set) {
          tmpPips = getStdDev()/configs.sl_std_dev_divider;
          if(Double.compare(tmpPips, configs.sl_pip*inst_pip) <=  0)  tmpPips = configs.sl_pip*inst_pip;
          slPrice = lastPrice + tmpPips;
          slPrice = SharedProps.round(slPrice, inst_scale + 1);
        }

        at = lastPrice;
        executeSubmitOrderProceed("DW", orderCmd, amountCurrent, at, tpPrice, slPrice);
      }
    } catch (Exception e) {
      SharedProps.print("IOrder call() E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
  }

  private void executeSubmitOrderProceed(String trend, OrderCommand cmd, double amount, double atPrice, double tpPrice, double slPrice){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          context.executeTask(new SubmitOrderProceed(trend, cmd, amount, atPrice, tpPrice, slPrice));
        } catch (Exception e) {
          SharedProps.print("executeSubmitOrderProceed call() E: "+e.getMessage()+" " +
              "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
        }
      }
    }).start();
    inst_exec_ts = SharedProps.get_sys_ts();
  }
  private class SubmitOrderProceed implements Callable<IOrder> {
    private final double amt, at, tp, sl;
    private final OrderCommand cmd;
    private final String trend;

    public SubmitOrderProceed(String dir, OrderCommand orderCmd, double amount, double atPrice, double tpPrice, double slPrice) {
      trend = dir;
      cmd = orderCmd;
      amt = amount;
      at = atPrice;
      tp = tpPrice;
      sl = slPrice;
    }
    public IOrder call() {
      IOrder new_order=null;
      try {
        long ts = SharedProps.get_sys_ts();
        if(Double.compare(tp, 0) == 0 && Double.compare(sl, 0) == 0)
          new_order = engine.submitOrder("O"+ts+"_"+trend, inst, cmd, amt);
        else
          new_order = engine.submitOrder("O"+ts+"_"+trend, inst, cmd, amt,
                                          at, configs.slippage, sl, tp, 0, o_comment(cmd));

        SharedProps.print(inst_str+"-"+cmd.toString()+"" +" amt:"+amt+ " at:"+at+" sl:"+sl+" tp:"+tp+
                          " info:"+inst_trend_info.get(inst_str+"_"+trend));
        inst_trend_info.put(inst_str+"_"+trend, "");

        SharedProps.curr_a_ts.put(inst_cur_1, ts + configs.open_new_for_currency_after_mins);
        SharedProps.curr_a_ts.put(inst_cur_2, ts + configs.open_new_for_currency_after_mins);

        if(new_order != null && new_order.getId() != null)
          SharedProps.oBusy.put(new_order.getId(), ts + 2500);

      }catch (Exception e){
        SharedProps.print("SubmitOrderProceed call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
      }
      inst_exec_ts = SharedProps.get_sys_ts();
      return new_order;
    }
  }

  //
  private double inst_pip_cost(OrderCommand cmd){
    try { 
      double v = SharedProps.acc_man.context.getUtils().convertPipToCurrency(inst, configs.account_currency, 
        (cmd==OrderCommand.BUY?OfferSide.ASK:OfferSide.BID))*10000;
      return v;
    } catch (Exception e) {
      SharedProps.print("convertPipToCurrency E: "+e.getMessage()+" " +
          "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
    return 0;
  }  
  private String o_comment(OrderCommand cmd){
    return o_comment(inst_pip_cost(cmd));
  }
  private String o_comment(double cost){
    return Double.compare(cost, 0.0) == 0
            ? ""
            : Double.toString(SharedProps.round(cost, 5));
  }
  private double o_parse_comment(String c){
    double pip_cost = 0;
    try{ 
      pip_cost = Double.parseDouble(c);
      if(Double.isNaN(pip_cost))pip_cost = 0;
    }catch (Exception e){}
    return pip_cost;
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

  //
  private void setInstFirstStep(long ts) {
    try {
      double step = (getLastTick().getAsk()/(inst_pip/10))*0.00001;
      step = configs.tp_step_1st_min*(step<1?(1/step):step);
      minFirstStepByInst = Double.compare(step, configs.tp_step_1st_min)>0?step:configs.tp_step_1st_min;
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
    double minimal = minFirstStepByInst*configs.period_to_minutes(configs.std_dev_period)*(inst_pip/10);
    cur_std_dev = Double.compare(cur_std_dev, minimal)>0?cur_std_dev:minimal;

    if (Double.isNaN(SharedProps.inst_std_dev_avg.get(inst_str)))
      SharedProps.inst_std_dev_avg.put(inst_str, cur_std_dev);
    else
      SharedProps.inst_std_dev_avg.put(inst_str, (SharedProps.inst_std_dev_avg.get(inst_str)+cur_std_dev)/2);
    inst_std_dev_ts = ts;
  }

  //
  private double getFirstStep() {
    double step = minFirstStepByInst/configs.tp_step_1st_divider;
    return (Double.compare(step, configs.tp_step_1st_min)>0?step:configs.tp_step_1st_min)*inst_pip;
  }
  private double getMinStdDev(double rnd) {
    double min = minFirstStepByInst;
    min = (Double.compare(min, configs.tp_step_1st_min)>0?min:configs.tp_step_1st_min)*inst_pip;
    min *= configs.profitable_ratio_min+(configs.profitable_ratio_max-configs.profitable_ratio_min)*rnd;
    min += getOffersDifference();
    return SharedProps.round(min,inst_scale+1);
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
      SharedProps.print(inst_str+" pip_dist: "+ SharedProps.round(pips,inst_scale+1)+"@"+tf);
    return pips;
  }

  //
  private boolean getTrend(String trend) {
    if (getCCI(trend)) {
      if (getPriceDifference(trend) || getEMAOverlap(trend)) {
        if (getMA(trend)) {
          inst_trend_info.put(inst_str+"_"+trend,
              inst_trend_info.get(inst_str+"_"+trend)
                +" tf:" + SharedProps.round(inst_timeframe, 1));
          return true;
        }
      }
    }
    inst_trend_info.put(inst_str+"_"+trend, "");
    return false;
  }


  private boolean getCCI(String trend) {
    if(configs.withoutCCI) return true;

    OfferSide side = trend.equals("DW") ? OfferSide.BID : OfferSide.ASK;
    try{
      double[] cci;   
      
      double timePeriodCCI = inst_timeframe*1;
      long to = history.getBarStart(configs.minutes_to_period(timePeriodCCI), getLastTick().getTime());
      long from = to-configs.minutes_to_period(timePeriodCCI).getInterval();

      cci = indicators.cci(inst, configs.minutes_to_period(timePeriodCCI) , side, configs.minutes_to_period_scale((timePeriodCCI)), Filter.WEEKENDS, from, to);
      if(Double.compare(cci[1], -1*configs.CCIlevelStart) < 0 && Double.compare(cci[1],-1*configs.CCIlevel) > 0 && Double.compare(cci[0],cci[1]) > 0) {
        inst_trend_info.put(inst_str+"_"+trend,
            inst_trend_info.get(inst_str+"_"+trend)
                +" CCI: "+SharedProps.round(cci[0],0)+">"+SharedProps.round(cci[1],0)+
                " "+configs.minutes_to_period(timePeriodCCI).toString()+":"+
                configs.minutes_to_period_scale((timePeriodCCI)));
        return trend.equals("DW");
      }else if(Double.compare(cci[1],configs.CCIlevelStart) > 0 && Double.compare(cci[1],configs.CCIlevel) < 0 && Double.compare(cci[0],cci[1]) < 0){
        inst_trend_info.put(inst_str+"_"+trend,
            inst_trend_info.get(inst_str+"_"+trend)
                +" CCI: "+SharedProps.round(cci[0],0)+"<"+SharedProps.round(cci[1],0)+
                " "+configs.minutes_to_period(timePeriodCCI).toString()+":"+
                configs.minutes_to_period_scale((timePeriodCCI)));
        return trend.equals("UP");
      }

    } catch (Exception e) {
      if(configs.debug)
        SharedProps.print("getCCI call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
    return false;
  }


  private boolean getMA(String trend) {
    if(configs.withoutMA) return true;

    try{
      OfferSide side = trend.equals("DW") ? OfferSide.BID : OfferSide.ASK;
      AppliedPrice appliedPrice = trend.equals("DW") ? AppliedPrice.HIGH: AppliedPrice.LOW;
      
      int numBars;
      double timePeriodMA;
      long to,from;
      double[] ma;
        
      numBars = 1;
      //configs.minutes_to_period(timePeriodMA).getInterval()*numBars
      timePeriodMA = inst_timeframe/configs.ma_time_divider;

      to = history.getBarStart(configs.minutes_to_period(timePeriodMA), getLastTick().getTime());
      from = to-configs.minutes_to_period(timePeriodMA).getInterval()*numBars;
      ma = indicators.sma(inst, configs.minutes_to_period(timePeriodMA) , side, appliedPrice,
          configs.minutes_to_period_scale(timePeriodMA),
          Filter.WEEKENDS, from, to);
      /*
      ma = indicators.kama(inst, configs.minutes_to_period(timePeriodMA), side, appliedPrice,
                configs.minutes_to_period_scale(timePeriodMA),
                configs.minutes_to_period_scale(3),
                configs.minutes_to_period_scale(timePeriodMA/3),
                Filter.WEEKENDS, from, to);
                */
      //ma = indicators.dema(inst, configs.minutes_to_period(timePeriodMA) , side, appliedPrice, configs.minutes_to_period_scale((timePeriodMA)), Filter.WEEKENDS, from, to)

      if(configs.debug)
        SharedProps.print("getMA: "+inst_str+"_"+trend+" "+ma[0]+(trend.equals("DW")?">":"<")+ma[numBars]
        +" "+configs.minutes_to_period_scale(timePeriodMA)+"@"+configs.minutes_to_period(timePeriodMA));

      if(Double.compare(ma[0], ma[numBars]) > 0) {
        inst_trend_info.put(inst_str+"_"+trend,
            inst_trend_info.get(inst_str+"_"+trend)+
                " ma:"+SharedProps.round(ma[0],inst_scale+1)+
                ">"+SharedProps.round(ma[numBars],inst_scale+1)+
                " "+configs.minutes_to_period(timePeriodMA).toString()+
                ":"+configs.minutes_to_period_scale((timePeriodMA)));
        return trend.equals("DW");
      }else if(Double.compare(ma[0], ma[numBars]) < 0) {
        inst_trend_info.put(inst_str+"_"+trend,
            inst_trend_info.get(inst_str+"_"+trend)+
                " ma:"+SharedProps.round(ma[0],inst_scale+1)+
                "<"+SharedProps.round(ma[numBars],inst_scale+1)+
                " "+configs.minutes_to_period(timePeriodMA).toString()+
                ":"+configs.minutes_to_period_scale((timePeriodMA)));
        return trend.equals("UP");
      }else
        return false;

    } catch (Exception e) {
      if(configs.debug)
        SharedProps.print("getMA call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
    }
    return false;
  }


  private boolean getPriceDifference(String trend) {
    if(!configs.active_price_dif) return false;
    if(configs.withoutPriceDifference) return true;
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

      double pipDistanceSet = SharedProps.round(inst_pip_dist*configs.price_diff_multiplier, inst_scale+1);
      
      if(configs.debug)
        SharedProps.print("getPriceDifference: "+inst_str+"_"+trend +
          " "+SharedProps.round(price_dif - pipDistanceSet, inst_scale+1) +
          " "+SharedProps.round(price_dif, inst_scale+1) +
          ">" + SharedProps.round(pipDistanceSet, inst_scale+1) +" @"+inst_timeframe);
      
      if(Double.compare(price_dif, pipDistanceSet) >= 0) {
        inst_trend_info.put(inst_str+"_"+trend,
          inst_trend_info.get(inst_str+"_"+trend)+
            " "+price_dif+">"+pipDistanceSet +
            " "+configs.minutes_to_period(inst_timeframe).toString()+
            ":"+configs.minutes_to_period_scale((inst_timeframe))+
            " / "+configs.minutes_to_period(inst_timeframe,200).toString()+
              ":"+configs.minutes_to_period_scale(inst_timeframe,200));
        return true;
       }
      
    } catch (Exception e) {
      if(configs.debug)
        SharedProps.print("getPriceDifference E: "+e.getMessage()+
                          " Thread: "+Thread.currentThread().getName()+" "+e+" "+inst_str);
    }
    return false;
  }


  private boolean getEMAOverlap(String trend) {
    if(!configs.active_ema_overlap) return false;

    try{
      OfferSide side = trend.equals("DW") ? OfferSide.BID : OfferSide.ASK;
      AppliedPrice applied = trend.equals("DW") ? AppliedPrice.LOW: AppliedPrice.HIGH;
      double last_price = trend.equals("DW") ? getLastTick().getBid(): getLastTick().getAsk();

      int num_bars;
      double lvl_1, lvl_2, lvl_3;
      long to,from;
      double[] ma;

      num_bars = configs.ema_overlap_bars_compared;
      lvl_1 = inst_timeframe*configs.ema_overlap_lvl_1;
      lvl_2 = inst_timeframe*configs.ema_overlap_lvl_2;
      lvl_3 = inst_timeframe*configs.ema_overlap_lvl_3;

      to = history.getBarStart(configs.minutes_to_period(lvl_1), getLastTick().getTime());
      from = to-configs.minutes_to_period(lvl_1).getInterval()*num_bars;
      ma = indicators.ema(inst, configs.minutes_to_period(lvl_1) , side, applied,
          configs.minutes_to_period_scale(lvl_1), Filter.WEEKENDS, from, to);
      if(configs.debug)
        SharedProps.print("getEMAOverlap lvl_1: "+inst_str+"_"+trend+
            " "+ma[0]+(trend.equals("DW")?"<":">")+ma[num_bars]+
            " "+configs.minutes_to_period_scale(lvl_1)+"@"+configs.minutes_to_period(lvl_1)+
            " "+ma[0]+(trend.equals("DW")?">":"<")+last_price);

      if(trend.equals("DW")){
        if(Double.compare(ma[0], ma[num_bars]) > 0 || Double.compare(ma[0],last_price) < 0)
          return false;

      }else if(trend.equals("UP")) {
        if(Double.compare(ma[0], ma[num_bars]) < 0 || Double.compare(ma[0],last_price) > 0)
          return false;
      }
      //
      if(configs.ema_overlap_lvl_2_active) {
        to = history.getBarStart(configs.minutes_to_period(lvl_2), getLastTick().getTime());
        from = to-configs.minutes_to_period(lvl_2).getInterval()*num_bars;
        ma = indicators.ema(inst, configs.minutes_to_period(lvl_2) , side, applied,
            configs.minutes_to_period_scale(lvl_2), Filter.WEEKENDS, from, to);
        if(configs.debug)
          SharedProps.print("getEMAOverlap lvl_2: "+inst_str+"_"+trend+
              " "+ma[0]+(trend.equals("DW")?"<":">")+ma[num_bars]+
              " "+configs.minutes_to_period_scale(lvl_2)+"@"+configs.minutes_to_period(lvl_2)+
              " "+ma[0]+(trend.equals("DW")?">":"<")+last_price);

        if(trend.equals("DW")){
          if(Double.compare(ma[0], ma[num_bars]) > 0 || Double.compare(ma[0],last_price) < 0)
            return false;

        }else if(trend.equals("UP")) {
          if(Double.compare(ma[0], ma[num_bars]) < 0 || Double.compare(ma[0],last_price) > 0)
            return false;
        }
      }
      //

      to = history.getBarStart(configs.minutes_to_period(lvl_3), getLastTick().getTime());
      from = to-configs.minutes_to_period(lvl_3).getInterval()*num_bars;
      ma = indicators.ema(inst, configs.minutes_to_period(lvl_3) , side, applied,
          configs.minutes_to_period_scale(lvl_3), Filter.WEEKENDS, from, to);
      if(configs.debug)
        SharedProps.print("getEMAOverlap lvl_3: "+inst_str+"_"+trend+
            " "+ma[0]+(trend.equals("DW")?">":"<")+ma[num_bars]+
            " "+configs.minutes_to_period_scale(lvl_3)+"@"+configs.minutes_to_period(lvl_3)+
            " "+ma[0]+(trend.equals("DW")?"<":">")+last_price);

      if(trend.equals("DW")){
        if(Double.compare(ma[0], ma[num_bars]) < 0 || Double.compare(ma[0],last_price) > 0)
          return false;
        return true;

      }else if(trend.equals("UP")) {
        if(Double.compare(ma[0], ma[num_bars]) > 0 || Double.compare(ma[0],last_price) < 0)
          return false;
        return true;
      }
      //

      return false;
    } catch (Exception e) {
      if(configs.debug)
        SharedProps.print("getEMAOverlap call() E: "+e.getMessage()+" " +
            "Thread: " + Thread.currentThread().getName() + " " + e +" " +inst_str);
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
      if(!order.getInstrument().toString().equals(inst_str))
        return;

      switch(message.getType()) {

        case ORDER_SUBMIT_OK :
          SharedProps.print("Order submit ok: " + message.getOrder());
          break;
        case ORDER_SUBMIT_REJECTED :
          SharedProps.print("Order submit rejected: " + message.getOrder());
          SharedProps.curr_a_ts.put(inst_cur_1, SharedProps.get_sys_ts()+10000);
          SharedProps.curr_a_ts.put(inst_cur_2, SharedProps.get_sys_ts()+10000);
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
          break;
        case ORDER_FILL_REJECTED :
          SharedProps.print("Order fill rejected: " + message.getOrder());
          SharedProps.curr_a_ts.put(inst_cur_1, SharedProps.get_sys_ts()+10000);
          SharedProps.curr_a_ts.put(inst_cur_2, SharedProps.get_sys_ts()+10000);
          break;

        case STOP_LOSS_LEVEL_CHANGED :
          SharedProps.print("ST changed ok: " + message.getOrder());
          break;
           
        case ORDER_CLOSE_OK :
          SharedProps.print("Order close ok: " + message.getOrder());

          double std_dev = getStdDevPip();
          if(Double.isNaN(std_dev)) return;

          if(Double.compare(order.getProfitLossInPips(), std_dev/configs.open_opposite_std_dev_divider) >= 0) {
            boolean buyOrder = true;
            boolean sellOrder =true;
            for( IOrder o : engine.getOrders(inst)) {
              if (o.getOrderCommand() == OrderCommand.BUY) buyOrder= false;
              else if(o.getOrderCommand() == OrderCommand.SELL)  sellOrder= false;
            }
            OrderCommand cmd = order.getOrderCommand();
            if((cmd==OrderCommand.SELL && buyOrder) || (cmd==OrderCommand.BUY && sellOrder)) {
              inst_trend_info.put(inst_str+"_"+((cmd==OrderCommand.BUY)?"DW":"UP"),
                  "OrderByOpoSide prev "+((cmd==OrderCommand.BUY)?"UP":"DW")+
                        " P/L:"+order.getProfitLossInPips()+"pip");
              inst_exec_ts = SharedProps.get_sys_ts();
              executeSubmitOrder(getAmount(1),
                cmd==OrderCommand.BUY?OrderCommand.SELL:OrderCommand.BUY);
            }
          }
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
    double amt = configs.get_amount();
    if(configs.amount_start_small) {
      if(num_orders == 1)
        num_orders = 1;
      amt *= (configs.merge_max * num_orders) / ((configs.merge_max * num_orders) + 2);
    }
    if (Double.compare(amt, 0.001) < 0)
      amt = 0.001;
    /*
    else if (Double.compare(amt, inst.getMaxTradeAmount()) > 0)
      amt = inst.getMaxTradeAmount();
    */
  
    /*
    SharedProps.print(
      "getAmount " + inst_str + " amt_i=" +amt_i + " incr=" + inst.getTradeAmountIncrement() +
      " min=" + inst.getMinTradeAmount() + " max=" + inst.getMaxTradeAmount() +
      " amt=" + SharedProps.round(amt * ( 0.001 * inst.getTradeAmountIncrement() ), 9)
    );
    */
    return amt;
    //double amt = SharedProps.round(configs.get_amount()*getAmtRatioByAccCurrency(), 6);
    //return (Double.compare(amt, 0.001000) < 0) ? 0.001 : (Double.compare(amt, 10)>0 ? 10: amt);
  }
  private double getAmtRatioByAccCurrency(){
    if(!SharedProps.inst_amt_ratio.containsKey(inst_str))
      return 1;
    return SharedProps.inst_amt_ratio.get(inst_str);
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