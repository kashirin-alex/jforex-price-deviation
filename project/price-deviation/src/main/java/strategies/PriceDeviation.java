// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.system.IClient;
import com.dukascopy.api.Instrument;
import java.util.concurrent.ConcurrentHashMap;


public class PriceDeviation {

  public static void main(String[] args) {

    if(java.lang.System.getProperty("STRATEGY") == null)
      SharedProps.strategy_dir="";
    else
      SharedProps.strategy_dir = java.lang.System.getProperty("STRATEGY");
    if(!SharedProps.strategy_dir.isEmpty() && !SharedProps.strategy_dir.endsWith("/"))
      SharedProps.strategy_dir+="/";

    StrategyConfigs.instruments = StrategyConfigs.getStrategyInstruments();
    SharedProps.configs.getConfigs();

    SharedProps.print("args: ");
    for(String s : args)
      SharedProps.print(s);

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        PriceDeviation.start_strategy();
      }
    });
    thread.start();
  }

  public static void start_strategy() {
    jForexClient acc_client = new jForexClient(
      System.getProperty("USR"),
      System.getProperty("PWD"),
      System.getProperty("ACC"),
      System.getProperty("cache_dir")
    );

    long strategy_check_ts = 0;
    IClient client = null;
    AccountManagement acc_man = null;

    PriceDeviationInstrument strategy;
    IClient s_client;
    ConcurrentHashMap<String, PriceDeviationInstrument> strategies= new ConcurrentHashMap<>();
    ConcurrentHashMap<String, IClient> clients= new ConcurrentHashMap<>();

    while(true) {

      try {
        Thread.sleep(10000);
      } catch (Exception e) {
        SharedProps.print(e);
      }

      if(System.currentTimeMillis() - strategy_check_ts < 600000)
        continue;
      strategy_check_ts = System.currentTimeMillis();

      //if (client!=null && !client.isConnected()) {
      //    SharedProps.print("Client Not Connected!");
      //}
      if(acc_man == null) {
        if(client == null) {
          client = acc_client.get_client(null);
          if(client == null)
            continue;
        }
        SharedProps.print("Starting strategy AccountManagement\n");
        acc_man = new AccountManagement();
        acc_man.strategyId = client.startStrategy(acc_man);
        acc_man.client = client;
        SharedProps.acc_man = acc_man;
        SharedProps.print("Started strategy AccountManagement\n");
        try {
          Thread.sleep(3000);
        } catch (Exception e) {
          SharedProps.print(e);
        }

      } else if(acc_man.pid_restart.get()){
        SharedProps.print("Strategy restarting AccountManagement\n");
        acc_man.stop_run.set(true);
        try{
          acc_man.client.stopStrategy(acc_man.strategyId);
        }catch (Exception e){}
        acc_man = null;
        if(!client.isConnected())
          client = null;
        continue;
      }
      acc_man.pid_restart.set(true);

      for(Instrument inst : StrategyConfigs.instruments) {
        if(!strategies.containsKey(inst.toString())) {
          if(!clients.containsKey(inst.toString())) {
            s_client = acc_client.get_client(inst);
            if(s_client == null)
              continue;
            clients.put(inst.toString(), s_client);
          }
          s_client = clients.get(inst.toString());

          SharedProps.print("Starting strategy "+inst.toString());
          strategy = new PriceDeviationInstrument();
          strategy.inst = inst;
          strategy.strategyId = s_client.startStrategy(strategy);
          strategy.client = s_client;
          strategies.put(inst.toString(), strategy);

          SharedProps.print("Started strategy "+inst.toString() +":"+ strategy.strategyId);
          SharedProps.print("Client "+inst.toString()+ " strategies count: "+ s_client.getStartedStrategies().size());

        } else {
          strategy = strategies.get(inst.toString());
          if(strategy.pid_restart.get()) {
            SharedProps.print("Strategy restarting "+inst.toString());
            strategy.stop_run.set(true);
            try{
              strategy.client.stopStrategy(strategy.strategyId);
            }catch (Exception e){}

            if(strategy.inst_thread != null){
              try {
                strategy.inst_thread.interrupt();
                Thread.sleep(10);
              } catch (Exception e) {
                SharedProps.print(e);
              }
            }
            strategies.remove(inst.toString());
          }
        }
        strategy.pid_restart.set(true);
      }
    }
  }

}