package _1ms.playtime.Handlers;

import _1ms.playtime.Main;

import java.sql.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class MySQLHandler {

    private final ConfigHandler configHandler;
    private final Main main;
    public MySQLHandler(ConfigHandler configHandler, Main main) {
        this.configHandler = configHandler;
        this.main = main;
    }

    public Connection conn;
    public boolean openConnection() {
        final String url = "jdbc:mariadb://" + configHandler.getADDRESS() +":" + configHandler.getPORT() + "/" + configHandler.getDB_NAME() + "?user=" + configHandler.getUSERNAME() + "&password=" + configHandler.getPASSWORD() + "&driver=org.mariadb.jdbc.Driver";
        try {
            conn = DriverManager.getConnection(url);
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS playtimes (uuid CHAR(36) PRIMARY KEY, time BIGINT NOT NULL DEFAULT 0, last_visit BIGINT)");
        } catch (SQLException e) {
            main.getLogger().error("Error while connecting to the database: {}", e.getMessage());
            return false;
        }
        return true;
    }

    public void saveData(final String uuid, final long time) {
        for(int i = 0; i < 2; i++) {
            try(PreparedStatement pstmt = conn.prepareStatement("INSERT INTO playtimes (uuid, time) VALUES (?, ?) ON DUPLICATE KEY UPDATE time = ?")) {
                pstmt.setString(1, uuid);
                pstmt.setLong(2, time);
                pstmt.setLong(3, time);
                pstmt.executeUpdate();
                break;
            } catch (SQLException e) {
                if(e instanceof SQLNonTransientConnectionException) { //If the conn was dropped, try to reopen it once.
                    openConnection();
                    continue;
                }
                throw new RuntimeException("Error while saving data into the database", e);
            }
        }
    }

    public void saveDataWithLv(final String uuid, final long time, final long last_visit) {
        for(int i = 0; i < 2; i++) {
            try(PreparedStatement pstmt = conn.prepareStatement("INSERT INTO playtimes (uuid, time, last_visit) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE time = ?, last_visit = ?")) {
                pstmt.setString(1, uuid);
                pstmt.setLong(2, time);
                pstmt.setLong(3, last_visit);
                pstmt.setLong(4, time);
                pstmt.setLong(5, last_visit);
                pstmt.executeUpdate();
                break;
            } catch (SQLException e) {
                if(e instanceof SQLNonTransientConnectionException) { //If the conn was dropped, try to reopen it once.
                    openConnection();
                    continue;
                }
                throw new RuntimeException("Error while saving data into the database", e);
            }
        }
    }

    public void saveDataOnlyLv(final String uuid, final long last_visit) {
        for(int i = 0; i < 2; i++) {
            try(PreparedStatement pstmt = conn.prepareStatement("INSERT INTO playtimes (uuid, last_visit) VALUES (?, ?) ON DUPLICATE KEY UPDATE last_visit = ?")) {
                pstmt.setString(1, uuid);
                pstmt.setLong(2, last_visit);
                pstmt.setLong(3, last_visit);
                pstmt.executeUpdate();
                break;
            } catch (SQLException e) {
                if(e instanceof SQLNonTransientConnectionException) { //If the conn was dropped, try to reopen it once.
                    openConnection();
                    continue;
                }
                throw new RuntimeException("Error while saving data into the database", e);
            }
        }
    }

    public long readData(final String uuid) {
        for(int i = 0; i < 2; i++) {
            try(PreparedStatement pstmt = conn.prepareStatement("SELECT time FROM playtimes WHERE uuid = ?")) {
                pstmt.setString(1, uuid);
                try(ResultSet rs = pstmt.executeQuery()) {
                    if(rs.next())
                        return rs.getLong("time");
                }
                return -1;
            } catch (SQLException e) {
                if(e instanceof SQLNonTransientConnectionException) {
                    openConnection();
                    continue;
                }
                throw new RuntimeException("Error while reading data from the database", e);
            }
        }
        main.getLogger().error("DB read error - Invalid state.");
        return -999;
    }

    public Iterator<String> getIterator() {
        final Set<String> playtimes = new HashSet<>();
        for (int i = 0; i < 2; i++) {
            try(ResultSet rs = conn.prepareStatement("SELECT uuid FROM playtimes").executeQuery()) {
                while (rs.next())
                    playtimes.add(rs.getString("uuid"));
                return playtimes.iterator(); //Fill up and then  ret
            } catch (SQLException e) {
                if(e instanceof SQLNonTransientConnectionException) {
                    openConnection();
                    playtimes.clear(); //CLear leftovers.
                    continue;
                }
                throw new RuntimeException("Error while reading data from the database", e);
            }
        }
        main.getLogger().error("DB IT error - Invalid state."); //Should never reach here?
        return null;
    }

    public void deleteAll() {
        for(int i = 0; i < 2; i++) {
            try(PreparedStatement pstmt = conn.prepareStatement("DELETE FROM playtimes")) {
                pstmt.executeUpdate();
                break;
            } catch (SQLException e) {
                if(e instanceof SQLNonTransientConnectionException) {
                    openConnection();
                    continue;
                }
                throw new RuntimeException("Error while saving data into the database",e);
            }
        }
    }

    public void closeConnection() {
        try {
            if (conn != null && !conn.isClosed())
                conn.close();
        } catch (SQLException e) {
            throw new RuntimeException("Error while closing connection with the database", e);
        }
    }
}
