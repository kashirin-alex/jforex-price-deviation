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

public class jForexClient {
  String jnlp_uri;
  String user_name;
  String password;
  File cache_dir = null;

  public jForexClient(String user, String pwd, String acc_type, String cache) {
    user_name = user;
    password = pwd;
    if(acc_type != null && acc_type.equals("live"))
      jnlp_uri = "https://www.dukascopy.com/client/live/jclient/jforex.jnlp";
    else
      jnlp_uri = "https://www.dukascopy.com/client/demo/jclient/jforex.jnlp";

    if(cache != null)
      cache_dir = new File(cache);
  }

  public IClient get_client(Instrument inst) {
    try {
      //get the instance of the IClient interface
      final IClient client = ClientFactory.getDefaultInstance(); //new DCClientImpl(); //

      if(cache_dir != null) 
        client.setCacheDirectory(cache_dir);

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
      while(client.getSubscribedInstruments().size() < instrumentsSet.size() && n > 0) {
        Thread.sleep(1000);
        instrumentsSubscribed = client.getSubscribedInstruments();
        if(inst == null){
          for(Instrument myInstrument : instrumentsSet) {
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