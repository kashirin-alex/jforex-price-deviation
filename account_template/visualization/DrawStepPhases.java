package jforex.charts;

import java.awt.Color;
import java.awt.Font;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.DayOfWeek;

import com.dukascopy.api.*;
import com.dukascopy.api.drawings.*;
import com.dukascopy.api.drawings.IScreenLabelChartObject.Corner;
import com.dukascopy.api.IEngine.OrderCommand;
import com.dukascopy.api.instrument.IMarketInfo;
import com.dukascopy.api.Configurable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;



public class DrawStepPhases implements IStrategy {

  private IContext        ctx;
  private IAccount        account;
  private IHistory        history;
  private IEngine         engine;
  private IDataService    dataService;
  private JFUtils         utils;

  @Configurable("zero_base")
  public double zero_base = 1.50;
  @Configurable("trail_step_1st_min")
  public double trail_step_1st_min = 1.0;
  @Configurable("trail_step_1st_divider")
  public double trail_step_1st_divider = 0.95;
  @Configurable("open_followup_step_offers_diff_devider")
  public double open_followup_step_offers_diff_devider = 9.00;
  @Configurable("open_followup_require_growth_rate")
  public double open_followup_require_growth_rate = 0.10;
  @Configurable("open_followup_step_first_muliplier")
  public double open_followup_step_first_muliplier = 4.80;
  
  @Configurable("open_followup_step_muliplier")
  public double open_followup_step_muliplier = 10.00;
  @Configurable("trail_step_rest_plus_gain")
  public double trail_step_rest_plus_gain = 0.20;

  @Configurable("ticks_history_secs")
  public long ticks_history_secs = 3600;
  @Configurable("open_followup_step_first_muliplier")
  public long ticks_history_ttl = 1800;

  private ConcurrentHashMap<String, Double> inst_offer_diff = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, Long> inst_offer_ts = new ConcurrentHashMap<>();

  @Override
  public void onStart(IContext context) throws JFException {
    ctx = context;
    account = context.getAccount();
    engine = context.getEngine();
    history = context.getHistory();
    dataService = context.getDataService();
    utils = ctx.getUtils();

    for(Instrument inst : ctx.getSubscribedInstruments()) {
      draw(inst, history.getLastTick(inst));
    }
    clear();
  }

  @Override
  public void onTick(Instrument inst, ITick tick) throws JFException {
    clear();
    draw(inst, tick);
  }

  @Override
  public void onBar(Instrument inst, Period period, IBar askBar, IBar bidBar) throws JFException { }

  @Override
  public void onMessage(IMessage message) throws JFException { }

  @Override
  public void onAccount(IAccount account) throws JFException { }

  @Override
  public void onStop() throws JFException {
    for (Instrument inst : ctx.getSubscribedInstruments()) {
      for (IChart chart : ctx.getCharts(inst)) {
        chart.removeAll();
      }
    }
  }

  public static double round(double amount, int decimalPlaces) {
    return (new BigDecimal(amount)).setScale(decimalPlaces, RoundingMode.HALF_UP).doubleValue();
  }


  public static double get_inst_amt_min(Instrument inst) {
    double amt_min = inst.getTradeAmountIncrement();
    amt_min /= 1000;
    if(inst.getGroup().isCrypto()) {
      amt_min /= 1000;
      amt_min *= (inst.getMinTradeAmount() / inst.getTradeAmountIncrement());
    }
    return amt_min;
  }

  public static int get_double_scale(double v) {
    if(Double.compare(v, 1.0) >= 0)
      return 0;
    if(Double.compare(v, 0.1) >= 0)
      return 1;
    if(Double.compare(v, 0.01) >= 0)
      return 2;
    if(Double.compare(v, 0.001) >= 0)
      return 3;
    if(Double.compare(v, 0.0001) >= 0)
      return 4;
    if(Double.compare(v, 0.00001) >= 0)
      return 5;
    if(Double.compare(v, 0.000001) >= 0)
      return 6;
    if(Double.compare(v, 0.0000001) >= 0)
      return 7;
    if(Double.compare(v, 0.00000001) >= 0)
      return 8;
    if(Double.compare(v, 0.000000001) >= 0)
      return 9;
    if(Double.compare(v, 0.0000000001) >= 0)
      return 10;
    if(Double.compare(v, 0.00000000001) >= 0)
      return 11;
    return 12;
  }

  private double get_offer_diff(Instrument inst, double diff, long tick_ts) {
    double max = 0.0;
    if(inst_offer_ts.getOrDefault(inst.toString(), 0L) > tick_ts) {
      max = inst_offer_diff.getOrDefault(inst.toString(), 0.0);
    } else {
      try {
        long ms = ticks_history_secs * 1000;
        for(ITick tick : history.getTicks(inst, tick_ts - ms, tick_ts)) {
          double v = tick.getAsk() - tick.getBid();
          if(Double.compare(max, v) < 0)
            max = v;
        }
        if(Double.compare(max, 0.0) > 0) {
          inst_offer_diff.put(inst.toString(), max);
          ms = ticks_history_ttl * 1000;
          inst_offer_ts.put(inst.toString(), ((long)(tick_ts/ms))*ms + ms);
        }
      } catch(Exception e) {
        max = inst_offer_diff.getOrDefault(inst.toString(), 0.0);
      }
    }
    if(Double.compare(max, diff) < 0) {
      inst_offer_diff.put(inst.toString(), max);
      return diff;
    }
    return (max + diff)/2;
  }


  public void draw(Instrument inst, ITick tick) throws JFException {
    if(tick == null)
      return;

    ITextChartObject           difference;
    IHorizontalLineChartObject line;
    long ts = System.currentTimeMillis();

    double inst_pip = inst.getPipValue();
    int inst_scale = inst.getPipScale();
    double lastTickAsk = tick.getAsk();
    double lastTickBid = tick.getBid();

    double diff_orig = (lastTickAsk - lastTickBid);
    double diff = get_offer_diff(inst, diff_orig, tick.getTime());
    diff_orig /= inst_pip;
    double diff_pip = diff/inst_pip;

    double step_1st_pip = (lastTickAsk / (inst_pip/10)) * 0.00001;
    if(Double.compare(step_1st_pip, 1.0) < 0)
      step_1st_pip = 1/step_1st_pip;
    step_1st_pip *= trail_step_1st_min;
    if(Double.compare(step_1st_pip, trail_step_1st_min) < 0)
      step_1st_pip = trail_step_1st_min;
    step_1st_pip /= trail_step_1st_divider;
    if(Double.compare(step_1st_pip, trail_step_1st_min) < 0)
      step_1st_pip = trail_step_1st_min;

    step_1st_pip = round(step_1st_pip, 1);
    double step_1st = round(step_1st_pip * inst_pip, inst_scale + 1);


    double inst_amt_min;
    int    inst_amt_scale;
    int    inst_amt_ratio;
    inst_amt_min = get_inst_amt_min(inst);
    inst_amt_scale = get_double_scale(inst_amt_min);
    inst_amt_min = round(inst_amt_min, inst_amt_scale);
    inst_amt_ratio = (int)round(1/inst_amt_min, 0);

    OrderCommand cmd;
    double amt_buy  = 0.0;
    double amt_sell = 0.0;
    int orders_buy  = 0;
    int orders_sell = 0;
    List<IOrder> orders = new ArrayList<>();

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
      }
    }
    amt_buy = round(amt_buy, inst_amt_scale + 3);
    amt_sell = round(amt_sell, inst_amt_scale + 3);

    final double amt_one = round(1.0 * inst_amt_min, inst_amt_scale + 3);
    final double require_growth = Math.abs((amt_buy - amt_sell)/amt_one) * open_followup_require_growth_rate;


    for (IChart chart : ctx.getCharts(inst)) {
      if(chart == null)
        continue;
      String prefix = inst.toString();

      difference = (ITextChartObject)chart.get(prefix + "DIFF");
      if(difference == null) {
        difference = chart.getChartObjectFactory().createText(prefix + "DIFF");
        difference.setFontColor(Color.white);
        difference.setTextAngle(0);
        difference.setOpacity(0.7f);
        chart.add(difference);
      }
      difference.setText("   " + round((diff)/inst.getPipValue(), 1) + "/" + round(diff_orig, 1) + "   ");
      difference.setTime(0, ts);
      difference.setPrice(0, diff/4 + tick.getBid());

      for(IOrder o : orders) {
        cmd = o.getOrderCommand();
        final String side = cmd + "-";
        double o_profit = o.getProfitLossInPips();
        double o_sl = o.getStopLossPrice();

        double require = require_growth;
        double step_flw_pip = step_1st_pip + diff_pip / open_followup_step_offers_diff_devider;

        boolean at_sl = Double.compare(o_sl, 0.0) > 0;
        if(at_sl) {
          require += open_followup_step_muliplier;
          final double flw_pos_pip = round((require * step_flw_pip) + (o_profit * trail_step_rest_plus_gain) + diff_pip + getCommisionPip(inst, o, ts, inst_amt_min, inst_amt_ratio), 1);

          final double flw_pos = flw_pos_pip * inst_pip;
          line = (IHorizontalLineChartObject)chart.get(prefix + side + "TRAIL");
          if(line == null) {
            line = chart.getChartObjectFactory().createHorizontalLine(prefix + side + "TRAIL");
            //line.setText(side + "Trail");
            line.setColor(Color.yellow);
            line.setLineStyle(LineStyle.DOT);
            line.setOpacity(0.7f);
            line.setLineWidth(1.0f);
            chart.add(line);
          }
          line.setTime(0, ts);
          line.setPrice(0, round(o.getOpenPrice() + (cmd == OrderCommand.BUY ? flw_pos : -flw_pos), inst_scale + 3));
          line.setText("" + flw_pos_pip + "     ");

          continue;
        }

        if(Double.compare(o_profit, 0.0) < 0)
          continue;

        double o_cost = diff_pip + Math.abs(step_1st_pip * zero_base);
        if(Double.compare(zero_base, 0.0) <= 0)
          o_cost *= -1;
        double zero_pos_pip = round(o_cost, 1);
        double zero_pos = o_cost * inst_pip;

        require += open_followup_step_first_muliplier;

        double flw_pos = 2 * diff_pip;
        if(Double.compare(zero_base, 0.0) > 0)
          flw_pos += o_cost;
        flw_pos += (require * step_flw_pip);

        double flw_pos_pip = round(flw_pos, 1);
        flw_pos *= inst_pip;

        if(cmd == OrderCommand.BUY) {
          zero_pos = round(o.getOpenPrice() + zero_pos, inst_scale + 3);
          flw_pos = round(o.getOpenPrice() + flw_pos, inst_scale + 3);
        } else {
          zero_pos = round(o.getOpenPrice() - zero_pos, inst_scale + 3);
          flw_pos = round(o.getOpenPrice() - flw_pos, inst_scale + 3);
        }

        line = (IHorizontalLineChartObject)chart.get(prefix + side + "ZERO");
        if(line == null) {
          line = chart.getChartObjectFactory().createHorizontalLine(prefix + side + "ZERO");
          //line.setText(side + "Zero");
          line.setColor(Color.orange);
          line.setLineStyle(LineStyle.DOT);
          line.setOpacity(0.7f);
          line.setLineWidth(1.0f);
          chart.add(line);
        }
        line.setTime(0, ts);
        line.setPrice(0, zero_pos);
        line.setText("" + zero_pos_pip + "     ");

        line = (IHorizontalLineChartObject)chart.get(prefix + side + "FLW");
        if(line == null) {
          line = chart.getChartObjectFactory().createHorizontalLine(prefix + side + "FLW");
          //line.setText(side + "Follow-Up");
          line.setColor(Color.green);
          line.setLineStyle(LineStyle.DOT);
          line.setOpacity(0.7f);
          line.setLineWidth(1.0f);
          chart.add(line);
        }
        line.setTime(0, ts);
        line.setPrice(0, flw_pos);
        line.setText("" + flw_pos_pip + "     ");
      }

    }

  }

  public void clear() throws JFException {
    for (IChart chart : ctx.getCharts()) {
      for(IChartObject obj : chart.getAll()) {
        if(!obj.isVisibleInWorkspaceTree()) {
          chart.remove(obj);
        }
      }
    }
  }

  private double getCommisionPip(Instrument inst, IOrder o, long ts, double inst_amt_min, double inst_amt_ratio) {
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
      chk = round(value/(pl/pip) + 0.1, 1);
    try {
      rate = utils.convertPipToCurrency(
        inst,
        account.getAccountCurrency(),
        (o.getOrderCommand() == OrderCommand.BUY ? OfferSide.ASK : OfferSide.BID)
      );
      if(!Double.isNaN(rate) && Double.compare(rate, 0.0) != 0) {
        rate *= o.getAmount() * inst_amt_ratio * (1000); // account_currency == pip-value (0.0001)
        rslt = round(value/rate + 0.1, 1);
      }
    } catch (Exception e) { }

    return round((Double.compare(rslt, 0.0) == 0 ? chk: rslt) * 2.2, 1);
  }

}
