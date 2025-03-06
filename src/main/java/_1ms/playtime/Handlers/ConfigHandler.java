package _1ms.playtime.Handlers;

import _1ms.playtime.Main;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import dev.dejvokep.boostedyaml.YamlDocument;
import dev.dejvokep.boostedyaml.block.implementation.Section;
import dev.dejvokep.boostedyaml.dvs.versioning.BasicVersioning;
import dev.dejvokep.boostedyaml.settings.dumper.DumperSettings;
import dev.dejvokep.boostedyaml.settings.general.GeneralSettings;
import dev.dejvokep.boostedyaml.settings.loader.LoaderSettings;
import dev.dejvokep.boostedyaml.settings.updater.UpdaterSettings;
import lombok.AccessLevel;
import lombok.Getter;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public class ConfigHandler {
    @Getter(AccessLevel.NONE)
    private final Main main;
    @Getter(AccessLevel.NONE)
    private YamlDocument dataConfig;
    @Getter(AccessLevel.NONE)
    private YamlDocument config;

    public ConfigHandler(Main main) {
        this.main = main;
    }

    private Component NO_CONSOLE_USE;
    private String YOUR_PLAYTIME;
    private Component NO_PLAYER;
    private String OTHER_PLAYTIME;
    private Component NO_PERMISSION;
    private Component CONFIG_RELOAD;
    private String PTRESET;
    private Component PTRESET_HELP;
    private Component PTRESETALL;
    private Component PTRESETALL_CONFIRM;
    private String PTSET;
    private Component INVALID_ARGS;
    private Component TOP_PLAYTIME_HEADER;
    private String TOP_PLAYTIME_LIST;
    private Component TOP_PLAYTIME_FOOTER;
    private String NO_SPAM;
    private String ADDRESS;
    private String DB_NAME;
    private String USERNAME;
    private String PASSWORD;
    private Component INVALID_VALUE;


    private int TOPLIST_LIMIT;
    private int SPAM_LIMIT;
    private int PORT;
    private boolean DATABASE;
    private boolean BSTATS;
    private boolean PRELOAD_PLACEHOLDERS;
    private boolean CHECK_FOR_UPDATES;
    private boolean USE_CACHE;
    private long CACHE_UPDATE_INTERVAL;
    private boolean VIEW_OWN_TIME;
    private boolean VIEW_OTHERS_TIME;
    private boolean VIEW_TOPLIST;
    private boolean isDataFileUpToDate;
    private boolean minimessage;
    private long genTime;
    private long start;
    private boolean OFFLINES_SHOULD_GET_REWARDS;

    private final TreeMap<Long, String> rewardsH = new TreeMap<>(); //TreeMap bc it needs to be ordered by the Long

    public void initConfig(@DataDirectory Path dataDirectory) {
        try {
            config = YamlDocument.create(new File(dataDirectory.toFile(), "config.yml"),
                    Objects.requireNonNull(getClass().getResourceAsStream("/config.yml")),
                    GeneralSettings.DEFAULT,
                    LoaderSettings.builder().setAutoUpdate(true).build(),
                    DumperSettings.DEFAULT,
                    UpdaterSettings.builder().setVersioning(new BasicVersioning("file-version"))
                            .setOptionSorting(UpdaterSettings.OptionSorting.SORT_BY_DEFAULTS).build()
            );

            config.update();
            config.save();

            makeNonChanging();
            makeConfigCache();

            if(!DATABASE)
                initDataConf(dataDirectory);
        } catch (IOException e) {
            throw new RuntimeException("Config initialize error. ", e); //Need to throw error, otherwise it won't unload.
        }
    }

    private void initDataConf(@DataDirectory Path dataDirectory) throws IOException {
        dataConfig = YamlDocument.create(new File(dataDirectory.toFile(), "playtimes.yml"),
                Objects.requireNonNull(getClass().getResourceAsStream("/playtimes.yml")),
                GeneralSettings.DEFAULT,
                LoaderSettings.builder().setAutoUpdate(true).build(),
                DumperSettings.DEFAULT,
                UpdaterSettings.builder().setVersioning(new BasicVersioning("file-version"))
                        .setOptionSorting(UpdaterSettings.OptionSorting.SORT_BY_DEFAULTS).build()
        );

        dataConfig.update();
        dataConfig.save();
    }

    private String getConfStr(final String path) {
        return config.getString(path).replace("\\n", "\n");
    }

    public void makeConfigCache() {
        minimessage = config.getString("Data.CONFIG_SERIALIZER").equals("MINIMESSAGE");
        NO_PERMISSION = initComp("Messages.NO_PERMISSION");
        NO_CONSOLE_USE = initComp("Messages.NO_CONSOLE_USE");
        YOUR_PLAYTIME = getConfStr("Messages.PLAYTIME_YOURS");
        NO_PLAYER = initComp("Messages.NO_PLAYER");
        OTHER_PLAYTIME = getConfStr("Messages.PLAYTIME_OTHERS");
        CONFIG_RELOAD = initComp("Messages.CONFIG_RELOAD");
        PTRESET = getConfStr("Messages.PTRESET");
        PTRESET_HELP = initComp("Messages.PTRESET_HELP");
        PTRESETALL = initComp("Messages.PTRESETALL");
        PTRESETALL_CONFIRM = initComp("Messages.PTRESETALL_CONFIRM");
        PTSET = getConfStr("Messages.PTSET");
        INVALID_ARGS = initComp("Messages.INVALID_ARGS");
        TOP_PLAYTIME_HEADER = initComp("Messages.TOP_PLAYTIME_HEADER");
        TOP_PLAYTIME_LIST = getConfStr("Messages.TOP_PLAYTIME_VALUES");
        TOP_PLAYTIME_FOOTER = initComp("Messages.TOP_PLAYTIME_FOOTER");
        NO_SPAM = getConfStr("Messages.NO_SPAM");
        SPAM_LIMIT = config.getInt("Data.SPAM_LIMIT");
        INVALID_VALUE = initComp("Messages.INVALID_VALUE");
        if(DATABASE) {
            ADDRESS = config.getString("Data.DATABASE.ADDRESS");
            PORT  = config.getInt("Data.DATABASE.PORT");
            DB_NAME  = config.getString("Data.DATABASE.DB_NAME");
            USERNAME = config.getString("Data.DATABASE.USERNAME");
            final String cfg = config.getString("Data.DATABASE.PASSWORD");
            PASSWORD = cfg == null ? "" : cfg;
        }

        VIEW_OWN_TIME = config.getBoolean("Data.PERMISSIONS.VIEW_OWN_TIME");
        VIEW_OTHERS_TIME = config.getBoolean("Data.PERMISSIONS.VIEW_OTHERS_TIME");
        VIEW_TOPLIST = config.getBoolean("Data.PERMISSIONS.VIEW_TOPLIST");
        if(!USE_CACHE)
            TOPLIST_LIMIT = config.getInt("Data.TOPLIST_LIMIT");
        //Rewards.
        OFFLINES_SHOULD_GET_REWARDS = config.getBoolean("Data.OFFLINES_SHOULD_GET_REWARDS");

        getConfigIterator("Rewards", false).forEachRemaining(key -> rewardsH.put(Long.valueOf(key), config.getString("Rewards." + key)));
        genTime = System.currentTimeMillis() - start;
    }

    public void makeNonChanging() {
        start = System.currentTimeMillis();
        USE_CACHE = config.getBoolean("Data.CACHING.USE_CACHE");
        CACHE_UPDATE_INTERVAL = config.getLong("Data.CACHING.CACHE_UPDATE_INTERVAL");
        BSTATS = config.getBoolean("Data.BSTATS");
        CHECK_FOR_UPDATES = config.getBoolean("Data.CHECK_FOR_UPDATES");
        isDataFileUpToDate = config.getBoolean("isDataFileUpToDate");
        DATABASE = config.getString("Data.DATA_METHOD").equals("DATABASE");
        PRELOAD_PLACEHOLDERS = config.getBoolean("Data.PRELOAD_PLACEHOLDERS");
        if(USE_CACHE)
            TOPLIST_LIMIT = config.getInt("Data.TOPLIST_LIMIT");
    }

    public Iterator<String> getConfigIterator(String path, boolean isData) {
        final Section section = isData ? dataConfig.getSection(path) : config.getSection(path);
        if(section == null) {
            return Collections.emptyIterator();
        } //Convert the config lib's Set<Object> into Iterator<String>
        return section.getKeys().stream().map(String::valueOf).collect(Collectors.toSet()).iterator();
    }

    public String getPermsUsageCount() {
        int i = 0;
        final String basePath = "Data.PERMISSIONS";
        Iterator<String> iterator = getConfigIterator(basePath, false);
        while (iterator.hasNext()) {
            String path = iterator.next();
            if(config.getBoolean(basePath + "." + path))
                i++;
            iterator.remove();
        }
        return String.valueOf(i);
    }

    public long getPtFromConfig(String name) {
        final Optional<Long> pt = dataConfig.getOptionalLong("Player-Data." + name + ".playtime");
        return pt.isPresent() ? pt.get() : -1;
    }

    public void savePtToConfig(String name, long time) {
        dataConfig.set("Player-Data." + name + ".playtime", time);
        saveData();
    }

    private void saveData() {
        try {
            dataConfig.save();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void modifyMainConfig(String path, Object value) {
        config.set(path, value);
        try {
            config.save();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void nullDataConfig() {
        dataConfig.set("Player-Data", null);
        saveData();
    }

    public void reloadConfig() {
        try {
            config.reload();
            makeConfigCache();
            if(DATABASE)
                main.loadDB();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Component initComp(final String path) { //Req from the conf file, only at startup once
        return decideNonComponent(config.getString(path).replace("\\n", "\n"));
    }

    public Component decideNonComponent(String message) {//Use to decide in runtime where replacements are needed.
        return minimessage ? MiniMessage.miniMessage().deserialize(message) : Component.text(message);
    }

    public String repL(String msg, long PlayTime) {
        return msg.replace("%weeks%", String.valueOf(main.calculatePlayTime(PlayTime, 'w')))
                .replace("%days%", String.valueOf(main.calculatePlayTime(PlayTime, 'd')))
                .replace("%hours%", String.valueOf(main.calculatePlayTime(PlayTime, 'h')))
                .replace("%minutes%", String.valueOf(main.calculatePlayTime(PlayTime, 'm')))
                .replace("%seconds%", String.valueOf(main.calculatePlayTime(PlayTime, 's')))
                .replace("%totaldays%", String.valueOf(main.calcTotalPT(PlayTime, 'd')))
                .replace("%totalhours%", String.valueOf(main.calcTotalPT(PlayTime, 'h')))
                .replace("%totalminutes%", String.valueOf(main.calcTotalPT(PlayTime, 'm')))
                .replace("%totalseconds%", String.valueOf(main.calcTotalPT(PlayTime, 's')));
    }

}
