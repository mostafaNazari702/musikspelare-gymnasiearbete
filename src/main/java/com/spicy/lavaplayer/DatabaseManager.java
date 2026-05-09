package com.spicy.lavaplayer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static DatabaseManager instance;
    private final String dbUrl;


    private DatabaseManager(String dbPath) {
        this.dbUrl = "jdbc:sqlite:" + dbPath;
        createTables();
        logger.info("DatabaseManager initierad med databas: {}", dbPath);
    }

    public static synchronized DatabaseManager init(String dbPath) {
        if (instance == null) {
            instance = new DatabaseManager(dbPath);
        }
        return instance;
    }


    public static synchronized DatabaseManager getInstance() {
        if (instance == null) {
            throw new IllegalStateException("DatabaseManager har inte initierats – anropa init() först");
        }
        return instance;
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(dbUrl);
    }

    private void createTables() {
        String createSearches = """
            CREATE TABLE IF NOT EXISTS searches (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                query      TEXT    NOT NULL,
                source     TEXT    NOT NULL,
                searched_at TEXT   NOT NULL
            )
            """;

        String createSearchResults = """
            CREATE TABLE IF NOT EXISTS search_results (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                search_id  INTEGER NOT NULL,
                position   INTEGER NOT NULL,
                track_id   TEXT,
                title      TEXT,
                author     TEXT,
                length     TEXT,
                uri        TEXT,
                FOREIGN KEY (search_id) REFERENCES searches(id) ON DELETE CASCADE
            )
            """;

        String createPlaybacks = """
            CREATE TABLE IF NOT EXISTS playbacks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id    TEXT,
                title       TEXT    NOT NULL,
                author      TEXT,
                length      TEXT,
                uri         TEXT,
                played_at   TEXT    NOT NULL
            )
            """;

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.execute("PRAGMA foreign_keys = ON");
            stmt.execute(createSearches);
            stmt.execute(createSearchResults);
            stmt.execute(createPlaybacks);
            logger.info("Databastabeller skapade / verifierade");
        } catch (SQLException e) {
            logger.error("Kunde inte skapa tabeller", e);
            throw new RuntimeException("Databasinitiering misslyckades", e);
        }
    }

    public long insertSearch(String query, String source, List<Map<String, Object>> results) {
        String insertSearch = "INSERT INTO searches (query, source, searched_at) VALUES (?, ?, ?)";
        String insertResult = "INSERT INTO search_results (search_id, position, track_id, title, author, length, uri) VALUES (?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = connect()) {
            conn.setAutoCommit(false);

            long searchId;
            try (PreparedStatement ps = conn.prepareStatement(insertSearch, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, query);
                ps.setString(2, source);
                ps.setString(3, LocalDateTime.now().format(DT_FMT));
                ps.executeUpdate();

                try (ResultSet keys = ps.getGeneratedKeys()) {
                    if (!keys.next()) throw new SQLException("Inget genererat id för sökning");
                    searchId = keys.getLong(1);
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(insertResult)) {
                int limit = Math.min(5, results.size());
                for (int i = 0; i < limit; i++) {
                    Map<String, Object> r = results.get(i);
                    ps.setLong(1, searchId);
                    ps.setInt(2, i + 1);
                    ps.setString(3, objToStr(r.get("id")));
                    ps.setString(4, objToStr(r.get("title")));
                    ps.setString(5, objToStr(r.get("author")));
                    ps.setString(6, objToStr(r.get("length")));
                    ps.setString(7, objToStr(r.get("uri")));
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            conn.commit();
            logger.info("Sökning sparad (id={}, query='{}', {} resultat)", searchId, query, Math.min(5, results.size()));
            return searchId;

        } catch (SQLException e) {
            logger.error("Kunde inte spara sökning", e);
            return -1;
        }
    }

    public long insertPlayback(String trackId, String title, String author, String length, String uri) {
        String sql = "INSERT INTO playbacks (track_id, title, author, length, uri, played_at) VALUES (?, ?, ?, ?, ?, ?)";

        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            ps.setString(1, trackId);
            ps.setString(2, title);
            ps.setString(3, author);
            ps.setString(4, length);
            ps.setString(5, uri);
            ps.setString(6, LocalDateTime.now().format(DT_FMT));
            ps.executeUpdate();

            try (ResultSet keys = ps.getGeneratedKeys()) {
                if (keys.next()) {
                    long id = keys.getLong(1);
                    logger.info("Uppspelning sparad (id={}, title='{}')", id, title);
                    return id;
                }
            }
        } catch (SQLException e) {
            logger.error("Kunde inte spara uppspelning", e);
        }
        return -1;
    }

    public List<Map<String, Object>> selectAllSearches() {
        String sql = "SELECT id, query, source, searched_at FROM searches ORDER BY id DESC";
        List<Map<String, Object>> list = new ArrayList<>();

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", rs.getLong("id"));
                row.put("query", rs.getString("query"));
                row.put("source", rs.getString("source"));
                row.put("searchedAt", rs.getString("searched_at"));
                list.add(row);
            }
        } catch (SQLException e) {
            logger.error("Kunde inte hämta sökningar", e);
        }
        return list;
    }

    public List<Map<String, Object>> selectSearchResults(long searchId) {
        String sql = "SELECT position, track_id, title, author, length, uri FROM search_results WHERE search_id = ? ORDER BY position";
        List<Map<String, Object>> list = new ArrayList<>();

        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setLong(1, searchId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("position", rs.getInt("position"));
                    row.put("trackId", rs.getString("track_id"));
                    row.put("title", rs.getString("title"));
                    row.put("author", rs.getString("author"));
                    row.put("length", rs.getString("length"));
                    row.put("uri", rs.getString("uri"));
                    list.add(row);
                }
            }
        } catch (SQLException e) {
            logger.error("Kunde inte hämta sökresultat för search_id={}", searchId, e);
        }
        return list;
    }

    public List<Map<String, Object>> selectAllPlaybacks() {
        String sql = "SELECT id, track_id, title, author, length, uri, played_at FROM playbacks ORDER BY id DESC";
        List<Map<String, Object>> list = new ArrayList<>();

        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("id", rs.getLong("id"));
                row.put("trackId", rs.getString("track_id"));
                row.put("title", rs.getString("title"));
                row.put("author", rs.getString("author"));
                row.put("length", rs.getString("length"));
                row.put("uri", rs.getString("uri"));
                row.put("playedAt", rs.getString("played_at"));
                list.add(row);
            }
        } catch (SQLException e) {
            logger.error("Kunde inte hämta uppspelningar", e);
        }
        return list;
    }


    public Map<String, Object> selectSearchWithResults(long searchId) {
        Map<String, Object> search = null;

        String sql = "SELECT id, query, source, searched_at FROM searches WHERE id = ?";
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, searchId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    search = new LinkedHashMap<>();
                    search.put("id", rs.getLong("id"));
                    search.put("query", rs.getString("query"));
                    search.put("source", rs.getString("source"));
                    search.put("searchedAt", rs.getString("searched_at"));
                }
            }
        } catch (SQLException e) {
            logger.error("Kunde inte hämta sökning med id={}", searchId, e);
        }

        if (search != null) {
            search.put("results", selectSearchResults(searchId));
        }
        return search;
    }

    public boolean updateSearchQuery(long searchId, String newQuery) {
        String sql = "UPDATE searches SET query = ? WHERE id = ?";

        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, newQuery);
            ps.setLong(2, searchId);
            int affected = ps.executeUpdate();
            if (affected > 0) {
                logger.info("Sökning uppdaterad (id={}, nyQuery='{}')", searchId, newQuery);
                return true;
            }
        } catch (SQLException e) {
            logger.error("Kunde inte uppdatera sökning id={}", searchId, e);
        }
        return false;
    }
    public boolean deleteSearch(long searchId) {
        String deleteResults = "DELETE FROM search_results WHERE search_id = ?";
        String deleteSearch  = "DELETE FROM searches WHERE id = ?";

        try (Connection conn = connect()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(deleteResults)) {
                ps.setLong(1, searchId);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = conn.prepareStatement(deleteSearch)) {
                ps.setLong(1, searchId);
                int affected = ps.executeUpdate();
                conn.commit();
                if (affected > 0) {
                    logger.info("Sökning raderad (id={})", searchId);
                    return true;
                }
            }
            conn.commit();
        } catch (SQLException e) {
            logger.error("Kunde inte radera sökning id={}", searchId, e);
        }
        return false;
    }
    public boolean deletePlayback(long playbackId) {
        String sql = "DELETE FROM playbacks WHERE id = ?";

        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setLong(1, playbackId);
            int affected = ps.executeUpdate();
            if (affected > 0) {
                logger.info("Uppspelning raderad (id={})", playbackId);
                return true;
            }
        } catch (SQLException e) {
            logger.error("Kunde inte radera uppspelning id={}", playbackId, e);
        }
        return false;
    }
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();

        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            // Antal sökningar
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM searches")) {
                stats.put("totalSearches", rs.next() ? rs.getLong("cnt") : 0);
            }
            // Antal uppspelningar
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM playbacks")) {
                stats.put("totalPlaybacks", rs.next() ? rs.getLong("cnt") : 0);
            }
            // Topp 5 söktermer
            String topSql = "SELECT query, COUNT(*) AS cnt FROM searches GROUP BY query ORDER BY cnt DESC LIMIT 5";
            List<Map<String, Object>> top = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(topSql)) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("query", rs.getString("query"));
                    row.put("count", rs.getLong("cnt"));
                    top.add(row);
                }
            }
            stats.put("topSearches", top);
            String topPlayedSql = "SELECT title, author, COUNT(*) AS cnt FROM playbacks GROUP BY title, author ORDER BY cnt DESC LIMIT 5";
            List<Map<String, Object>> topPlayed = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery(topPlayedSql)) {
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("title", rs.getString("title"));
                    row.put("author", rs.getString("author"));
                    row.put("count", rs.getLong("cnt"));
                    topPlayed.add(row);
                }
            }
            stats.put("topPlaybacks", topPlayed);

        } catch (SQLException e) {
            logger.error("Kunde inte hämta statistik", e);
        }
        return stats;
    }
    private static String objToStr(Object obj) {
        return obj != null ? obj.toString() : null;
    }
}