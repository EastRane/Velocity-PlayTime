package _1ms.playtime.Handlers;

import _1ms.playtime.Main;
import com.velocitypowered.api.proxy.Player;
import lombok.Getter;

import java.util.*;

public class CacheHandler {
    private final Main main;
    private final ConfigHandler configHandler;
    @Getter
    private long cacheGenTime;

    public CacheHandler(Main main, ConfigHandler configHandler) {
        this.main = main;
        this.configHandler = configHandler;
    }
    public void buildCache() {
        Iterator<Object> iterator = main.getIterator();
        if(iterator == null)
            return;
        long start = System.currentTimeMillis();
        main.getLogger().info("Building Cache...");
        final HashMap<String, Long> TempCache = new HashMap<>();
        while (iterator.hasNext()) {
            String Pname = iterator.next().toString();
            long Ptime = main.getSavedPt(Pname);
            TempCache.put(Pname, Ptime);
            iterator.remove();
        }

        for(int i = 0; i < configHandler.getTOPLIST_LIMIT(); i++) {
            Optional<Map.Entry<String, Long>> member = TempCache.entrySet().stream().max(Map.Entry.comparingByValue());
            if(member.isEmpty())
                break;
            Map.Entry<String, Long> Entry = member.get();
            main.playtimeCache.put(Entry.getKey(), Entry.getValue());
            TempCache.remove(Entry.getKey());
        }
        main.getLogger().info("The cache has been built, took: {} ms", cacheGenTime = (System.currentTimeMillis() - start) + configHandler.getGenTime());
    }

    public HashMap<String, Long> generateTempCache() {
        return new HashMap<>(main.playtimeCache);
    }

    public void upd2(String keyToRemove) {//Put next largest value
        Iterator<Object> iterator = main.getIterator();
        if(iterator == null)
            return;
        final HashMap<String, Long> TempCache = new HashMap<>(); //Load all values from config except the one given
        while (iterator.hasNext()) {
            String Pname = iterator.next().toString();
            if(Objects.equals(Pname, keyToRemove))
                continue;
            long Ptime = main.getSavedPt(Pname);
            TempCache.put(Pname, Ptime);
            iterator.remove();
        }

        Optional<Map.Entry<String, Long>> nextLargest = TempCache.entrySet().stream() //Put the next largest value to the cache
                .filter(entry -> !main.playtimeCache.containsKey(entry.getKey()))
                .max(Map.Entry.comparingByValue());

        nextLargest.ifPresent(entry -> main.playtimeCache.put(entry.getKey(), entry.getValue()));
    }

    public void updateCache() {//Runs at the interval defined in the config
        HashMap<String, Long> TempCache = generateTempCache();
        for (int i = 0; i < main.playtimeCache.size() - configHandler.getTOPLIST_LIMIT(); i++) { //Check players on the server only, not the toplist
            Optional<Map.Entry<String, Long>> member = TempCache.entrySet().stream().min(Map.Entry.comparingByValue());
            member.ifPresent(Entry -> {
                Optional<Player> player = main.getProxy().getPlayer(Entry.getKey());
                if(player.isEmpty()) {
                    main.playtimeCache.remove(Entry.getKey());
                }
                TempCache.remove(Entry.getKey());
            });
        }
    }
}
