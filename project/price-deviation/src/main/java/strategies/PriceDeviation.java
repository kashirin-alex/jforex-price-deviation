// author Kashirin Alex(kshirin.alex@gmail.com)

package strategies;

import com.dukascopy.api.system.IPreferences;
import com.dukascopy.api.system.ISystemListener;
import com.dukascopy.api.system.IClient;
import com.dukascopy.api.system.ClientFactory;

import com.dukascopy.api.Instrument;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PriceDeviation {
    private static String jnlp_uri;
    private static String user_name;
    private static String password;
    private static File cache_dir = null;

    public static void main(String[] args) {

        if(java.lang.System.getProperty("STRATEGY") == null) SharedProps.strategy_dir="";
        else SharedProps.strategy_dir = java.lang.System.getProperty("STRATEGY");
        if(!SharedProps.strategy_dir.isEmpty() && !SharedProps.strategy_dir.endsWith("/")) SharedProps.strategy_dir+="/";

        StrategyConfigs.instruments = StrategyConfigs.getStrategyInstruments();

        user_name = System.getProperty("USR");
        password = System.getProperty("PWD");
        if(System.getProperty("ACC") != null && System.getProperty("ACC").equals("live"))
            jnlp_uri = "https://www.dukascopy.com/client/live/jclient/jforex.jnlp";
        else
            jnlp_uri = "https://www.dukascopy.com/client/demo/jclient/jforex.jnlp";

        if(System.getProperty("cache_dir") != null)
		    cache_dir = new File(System.getProperty("cache_dir"));

        SharedProps.print("args: ");
        for (String s : args) SharedProps.print(s);

        try{
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    long strategy_check_ts = 0;

                    IClient client = null;
                    AccountManagement acc_man=null;

                    PriceDeviationInstrument strategy;
                    IClient s_client;
                    ConcurrentHashMap<String, PriceDeviationInstrument> strategies= new ConcurrentHashMap<>();
                    ConcurrentHashMap<String, IClient> clients= new ConcurrentHashMap<>();

                    while(true){
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                            SharedProps.print(e.getMessage());
                        }

                        if(SharedProps.offline_sleep!=0){
                            SharedProps.print("main sleep: "+(SharedProps.offline_sleep));
                            try{ Thread.sleep(SharedProps.offline_sleep); } catch (Exception e) {}
                            continue;
                        }

                        if(System.currentTimeMillis() - strategy_check_ts < 600000) continue;
                        strategy_check_ts = System.currentTimeMillis();

                        //if (client!=null && !client.isConnected()) {
                        //    SharedProps.print("Client Not Connected!");
                        //}
                        if(acc_man==null){
                            if(client==null){
                                client = get_client(null);
                            }
                            if(client==null)continue;
                            SharedProps.print("Starting strategy AccountManagement");
                            acc_man = new AccountManagement();
                            acc_man.strategyId = client.startStrategy(acc_man);
                            acc_man.client = client;
                            SharedProps.print("Started strategy AccountManagement");

                        }else if (acc_man.pid_restart){
                            SharedProps.print("Strategy restarting AccountManagement");
                            acc_man.stop_run=true;
                            try{
                                acc_man.client.stopStrategy(acc_man.strategyId);
                            }catch (Exception e){}
                            acc_man=null;
                            continue;
                        }
                        acc_man.pid_restart = true;

                        for( Instrument inst : StrategyConfigs.instruments){
                            if(!strategies.containsKey(inst.toString())){
                                if(!clients.containsKey(inst.toString())){
                                    s_client = get_client(inst);
                                    if(s_client==null) continue;
                                    clients.put(inst.toString(), s_client);
                                }
                                s_client = clients.get(inst.toString());
                                SharedProps.print("Starting strategy "+inst.toString());
                                strategy = new PriceDeviationInstrument();
                                strategy.inst = inst;
                                strategy.strategyId = s_client.startStrategy(strategy);
                                strategy.client = s_client;
                                SharedProps.print("Started strategy "+inst.toString() +":"+ strategy.strategyId);
                                SharedProps.print("Client "+inst.toString()+ " strategies count: "+ s_client.getStartedStrategies().size());
                                strategies.put(inst.toString(), strategy);

                            } else{
                                strategy = strategies.get(inst.toString());
                                if (strategy.pid_restart){
                                    SharedProps.print("Strategy restarting "+inst.toString());
                                    strategy.stop_run=true;
                                    try{
                                        strategy.client.stopStrategy(strategy.strategyId);
                                    }catch (Exception e){}

                                    if(strategy.inst_thread!=null){
                                        try {
                                            strategy.inst_thread.interrupt();
                                            Thread.sleep(10);
                                        } catch (Exception e) {
                                            SharedProps.print(e.getMessage());
                                        }
                                    }
                                    strategies.remove(inst.toString());
                                }
                            }
                            strategy.pid_restart = true;
                        }
                    }
                }
            });
            thread.start();

        } catch (Exception e) {
        }
    }


    private static IClient get_client(Instrument inst) {
        try {
            //get the instance of the IClient interface
            final IClient client = ClientFactory.getDefaultInstance(); //new DCClientImpl(); //

			if(cache_dir != null) client.setCacheDirectory(cache_dir);

            //set the listener that will receive system events
            client.setSystemListener(new ISystemListener() {
                private int lightReconnects = 3;

                public void onStart(long processId) {
                    //IConsole console = context.getConsole();
                    SharedProps.print("Client started: " + processId);
                }

                public void onStop(long processId) {
                    SharedProps.print("Client stopped: " + processId);
                    if (client.getStartedStrategies().size() == 0) {}
                }

                @Override
                public void onConnect() {
                    SharedProps.print("Connected");
                    lightReconnects = 5;
                }

                @Override
                public void onDisconnect() {
                    SharedProps.print("Disconnected");
                    Runnable runnable = new Runnable() {
                        @Override
                        public void run() {
                            while(lightReconnects > 0){
                                try {
                                    client.reconnect();
                                    Thread.sleep(2000);
                                    if(client.isConnected()) return;
                                    --lightReconnects;
                                    SharedProps.print("TRY TO RECONNECT, reconnects left: " + lightReconnects);
                                } catch (Exception e) {
                                    SharedProps.print("Failed to reconnect: " + e.getMessage());
                                }
                            }

                            while(!client.isConnected()) {
                                try {
                                    client.connect(jnlp_uri, user_name, password);
                                    Thread.sleep(10000);
                                } catch (Exception e) {
                                    SharedProps.print("Failed to connect: " + e.getMessage());
                                }
                            };
                        }
                    };
                    new Thread(runnable).start();
                }
            });

            SharedProps.print("Connecting...");
            //connect to the server using jnlp, user name and password

            //wait for it to connect
            int i = 10; //wait max ten seconds
            while (i > 0 && !client.isConnected()) {
                Thread.sleep(1000);
                client.connect(jnlp_uri, user_name, password);
                i--;
            }
            if (!client.isConnected()) {
                SharedProps.print("Failed to connect Dukascopy servers");
            }

            Set<Instrument> instrumentsSet = new HashSet<Instrument>();
            if(inst==null)
                for( Instrument myInstrument : StrategyConfigs.instruments  )  instrumentsSet.add(myInstrument);
            else
                instrumentsSet.add(inst);

            client.setSubscribedInstruments(instrumentsSet);

            Set<Instrument> instrumentsSubscribed;
            int n = 100;
            while (client.getSubscribedInstruments().size() < instrumentsSet.size() && n > 0) {
                Thread.sleep(1000);
                instrumentsSubscribed = client.getSubscribedInstruments();
                if(inst==null){
                    for( Instrument myInstrument : instrumentsSet) {
                        if(!instrumentsSubscribed.contains(myInstrument)) {
                            SharedProps.print("instrument not subscribed: "+ myInstrument.toString());
                        }
                    }
                }else if(!instrumentsSubscribed.contains(inst)) {
                    SharedProps.print("instrument not subscribed: "+ inst.toString());
                }
                SharedProps.print("Instruments Subscribe wait: " + n);
                n--;
            }

            IPreferences pref = client.getPreferences();
            pref.platform().platformSettings().skipTicks(false);
            SharedProps.print("Client isSkipTicks: " + pref.platform().platformSettings().isSkipTicks());

            return client;
		} catch (Exception e) {
            e.printStackTrace();
            System.out.print(e.getMessage());
		}
		return null;
    }
}