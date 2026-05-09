package com.spicy.lavaplayer;

import com.github.topi314.lavasrc.spotify.SpotifySourceManager;
import com.sedmelluq.discord.lavaplayer.format.StandardAudioDataFormats;
import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.player.DefaultAudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.player.AudioLoadResultHandler;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException;
import com.sedmelluq.discord.lavaplayer.track.*;
import dev.lavalink.youtube.YoutubeAudioSourceManager;
import io.github.cdimascio.dotenv.Dotenv;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import io.javalin.websocket.WsMessageContext;
import io.javalin.websocket.WsCloseContext;
import io.javalin.websocket.WsErrorContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.nio.file.Path;
import java.nio.file.Paths;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    
    private static final Map<String, WebSocketSession> activeWsSessions = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int MAX_CONCURRENT_TRANSCODERS = resolveMaxTranscoders();
    private static final Semaphore ffmpegPermits = new Semaphore(MAX_CONCURRENT_TRANSCODERS);

    private static int resolveMaxTranscoders() {
        String configured = Optional.ofNullable(System.getProperty("lavaplayer.maxTranscoders"))
            .orElse(System.getenv("LAVAPLAYER_MAX_TRANSCODERS"));
        if (configured != null) {
            try {
                int parsed = Integer.parseInt(configured.trim());
                if (parsed > 0) {
                    return parsed;
                }
                logger.warn("Invalid lavaplayer max transcoders '{}' (must be >0). Falling back to 4.", configured);
            } catch (NumberFormatException ex) {
                logger.warn("Unable to parse lavaplayer max transcoders '{}'. Falling back to 4.", configured, ex);
            }
        }
        return 4;
    }

    private static TranscoderPermit acquireTranscoderPermit(TranscoderBusyExceptionFactory exceptionFactory) throws TranscoderBusyException {
        try {
            if (!ffmpegPermits.tryAcquire(5, TimeUnit.SECONDS)) {
                throw exceptionFactory.create("Transcoder capacity reached, please retry shortly");
            }
            return new TranscoderPermit(ffmpegPermits);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw exceptionFactory.create("Interrupted while waiting for transcoder capacity");
        }
    }

    public static void main(String[] args) {
        AudioPlayerManager playerManager = new DefaultAudioPlayerManager();
        playerManager.getConfiguration().setOutputFormat(StandardAudioDataFormats.DISCORD_PCM_S16_LE);

        playerManager.registerSourceManager(new YoutubeAudioSourceManager(true));

        Dotenv dotenv = Dotenv.load();

        String spotifyClientId = dotenv.get("SPOTIFY_CLIENT_ID");
        String spotifyClientSecret = dotenv.get("SPOTIFY_CLIENT_SECRET");
        String spotifyCountry = dotenv.get("SPOTIFY_COUNTRY");
        try {
            String[] providers = new String[]{
                "ytsearch:\"%ISRC%\"",
                "ytsearch:%QUERY%"
            };
            SpotifySourceManager spotify = new SpotifySourceManager(
                providers,
                spotifyClientId,
                spotifyClientSecret,
                spotifyCountry,
                playerManager
            );
            playerManager.registerSourceManager(spotify);
            logger.info("Spotify source registered (country={})", spotifyCountry);
        } catch (Throwable t) {
            logger.error("Failed to initialize SpotifySourceManager. Check credentials/dependencies.", t);
        }

        Javalin app = Javalin.create(cfg -> {
            cfg.staticFiles.add(staticFiles -> {
                staticFiles.directory = "frontend";
                staticFiles.hostedPath = "/";
                staticFiles.location = Location.EXTERNAL;
            });
        }).start(7070);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { app.stop(); } catch (Exception ignored) {}
            try { playerManager.shutdown(); } catch (Exception ignored) {}
        }, "shutdown"));

        app.before(ctx -> {
            ctx.header("Access-Control-Allow-Origin", "*");
            ctx.header("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
            ctx.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
        });
        app.options("/*", ctx -> ctx.status(204));

        String dbPath = Paths.get(".", "musiksystem.db").toAbsolutePath().normalize().toString();
        DatabaseManager.init(dbPath);

        app.get("/", ctx -> ctx.redirect("/index.html"));
        app.get("/api/health", ctx -> ctx.json(Map.of("status", "ok")));
        app.get("/api/search", ctx -> handleSearch(ctx, playerManager));

        app.get("/api/db/searches", App::handleGetSearches);
        app.get("/api/db/searches/{id}", App::handleGetSearchById);
        app.get("/api/db/playbacks", App::handleGetPlaybacks);
        app.get("/api/db/stats", App::handleGetStats);
        app.delete("/api/db/searches/{id}", App::handleDeleteSearch);
        app.delete("/api/db/playbacks/{id}", App::handleDeletePlayback);
        
        app.ws("/ws/audio", ws -> {
            ws.onConnect(ctx -> handleWsConnect(ctx, playerManager));
            ws.onMessage(ctx -> handleWsMessage(ctx, playerManager));
            ws.onClose(ctx -> handleWsClose(ctx));
            ws.onError(ctx -> handleWsError(ctx));
        });

        logger.info("Server ready on http://localhost:7070");
        logger.info("WebSocket audio streaming available at ws://localhost:7070/ws/audio");
    }
    
    
    private static void handleWsConnect(WsContext ctx, AudioPlayerManager pm) {
        String sessionId = UUID.randomUUID().toString();
        WebSocketSession session = new WebSocketSession(sessionId, ctx, pm);
        activeWsSessions.put(sessionId, session);
        ctx.attribute("sessionId", sessionId);
        
        logger.info("WebSocket connected: {}", sessionId);
        
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "connected");
        msg.put("sessionId", sessionId);
        session.sendJson(msg);
    }
    
    private static final ObjectMapper wsObjectMapper = new ObjectMapper();
    
    private static void handleWsMessage(WsMessageContext ctx, AudioPlayerManager pm) {
        String sessionId = ctx.attribute("sessionId");
        WebSocketSession session = activeWsSessions.get(sessionId);
        
        if (session == null) {
            logger.warn("WebSocket message for unknown session: {}", sessionId);
            return;
        }
        
        try {
            String rawMessage = ctx.message();
            Map<String, Object> message = wsObjectMapper.readValue(
                rawMessage, new TypeReference<Map<String, Object>>() {});
            String type = (String) message.get("type");
            
            if (type == null) {
                session.sendError("Missing message type");
                return;
            }
            
            switch (type) {
                case "play" -> handleWsPlay(session, message, pm);
                case "pause" -> session.pause();
                case "resume" -> session.resume();
                case "stop" -> session.stopPlayback();
                default -> session.sendError("Unknown message type: " + type);
            }
        } catch (Exception e) {
            logger.error("Error handling WebSocket message", e);
            session.sendError("Error: " + e.getMessage());
        }
    }
    
    private static void handleWsPlay(WebSocketSession session, Map<String, Object> message, AudioPlayerManager pm) {
        String url = (String) message.get("url");
        if (url == null || url.isBlank()) {
            session.sendError("Missing url parameter");
            return;
        }
        
        session.stopPlayback();
        
        logger.info("Session {}: Play request for {}", session.getSessionId(), url);
        session.setState(WebSocketSession.State.LOADING);
        
        pm.loadItem(url, new AudioLoadResultHandler() {
            @Override
            public void trackLoaded(AudioTrack track) {
                startWsPlayback(session, track, pm);
            }
            
            @Override
            public void playlistLoaded(AudioPlaylist playlist) {
                if (!playlist.getTracks().isEmpty()) {
                    startWsPlayback(session, playlist.getTracks().get(0), pm);
                } else {
                    session.sendError("Empty playlist");
                    session.setState(WebSocketSession.State.IDLE);
                }
            }
            
            @Override
            public void noMatches() {
                session.sendError("No matches found");
                session.setState(WebSocketSession.State.IDLE);
            }
            
            @Override
            public void loadFailed(FriendlyException exception) {
                session.sendError("Load failed: " + exception.getMessage());
                session.setState(WebSocketSession.State.IDLE);
            }
        });
    }
    
    private static void startWsPlayback(WebSocketSession session, AudioTrack track, AudioPlayerManager pm) {
        String sessionId = session.getSessionId();
        
        TranscoderPermit permit;
        try {
            permit = acquireTranscoderPermit(TranscoderBusyException::new);
        } catch (TranscoderBusyException e) {
            session.sendError("Server busy: " + e.getMessage());
            session.setState(WebSocketSession.State.IDLE);
            return;
        }
        
        session.setPermit(permit);
        session.setCurrentTrack(track);
        session.getPlayer().playTrack(track);
        session.setState(WebSocketSession.State.PLAYING);
        
        session.sendMetadata(track.getInfo());

        try {
            AudioTrackInfo info = track.getInfo();
            DatabaseManager.getInstance().insertPlayback(
                info.identifier,
                info.title,
                info.author,
                formatTrackLength(info),
                info.uri
            );
        } catch (Exception dbEx) {
            logger.warn("Kunde inte spara uppspelning till databasen", dbEx);
        }
        
        logger.info("Session {}: Starting playback of '{}'", sessionId, track.getInfo().title);
        
        Thread streamThread = new Thread(() -> {
            StreamPcmToWsUtil.streamToWebSocket(session, permit);
        }, "ws-stream-" + sessionId);
        streamThread.setDaemon(true);
        session.setStreamingThread(streamThread);
        streamThread.start();
    }
    
    private static void handleWsClose(WsCloseContext ctx) {
        String sessionId = ctx.attribute("sessionId");
        if (sessionId != null) {
            WebSocketSession session = activeWsSessions.remove(sessionId);
            if (session != null) {
                session.close();
                logger.info("WebSocket closed: {} (status: {}, reason: {})", 
                    sessionId, ctx.status(), ctx.reason());
            }
        }
    }
    
    private static void handleWsError(WsErrorContext ctx) {
        String sessionId = ctx.attribute("sessionId");
        Throwable error = ctx.error();
        logger.warn("WebSocket error for session {}: {}", sessionId, 
            error != null ? error.getMessage() : "unknown");
        
        if (sessionId != null) {
            WebSocketSession session = activeWsSessions.get(sessionId);
            if (session != null) {
                session.close();
                activeWsSessions.remove(sessionId);
            }
        }
    }


    private static void handleSearch(Context ctx, AudioPlayerManager pm) {
        String src = Optional.ofNullable(ctx.queryParam("src")).filter(s -> !s.isBlank()).orElse("yt");
        String q = ctx.queryParam("q");
        if (q == null || q.isBlank()) { ctx.status(400).json(Map.of("error", "Missing q")); return; }
        String prefixed = "sp".equalsIgnoreCase(src) ? "spsearch:" + q : "ytsearch:" + q;

        CompletableFuture<List<Map<String, Object>>> fut = new CompletableFuture<>();
        pm.loadItem(prefixed, new AudioLoadResultHandler() {
            @Override public void trackLoaded(AudioTrack track) { fut.complete(List.of(trackToJson(track))); }
            @Override public void playlistLoaded(AudioPlaylist playlist) {
                List<Map<String, Object>> items = new ArrayList<>();
                int limit = Math.min(5, playlist.getTracks().size());
                for (int i = 0; i < limit; i++) items.add(trackToJson(playlist.getTracks().get(i)));
                fut.complete(items);
            }
            @Override public void noMatches() { fut.complete(List.of()); }
            @Override public void loadFailed(FriendlyException exception) { fut.completeExceptionally(exception); }
        });
        try {
            List<Map<String, Object>> items = fut.get(7, TimeUnit.SECONDS);
            ctx.json(Map.of("items", items));

            // Spara sökning + resultat i databasen
            try {
                DatabaseManager.getInstance().insertSearch(q, src, items);
            } catch (Exception dbEx) {
                logger.warn("Kunde inte spara sökning till databasen", dbEx);
            }
        }
        catch (Exception e) { ctx.status(500).json(Map.of("error", e.getMessage())); }
    }

    private static Map<String, Object> trackToJson(AudioTrack t) {
        AudioTrackInfo i = t.getInfo();

        String uri = (i.uri != null && !i.uri.isBlank()) ? i.uri : inferUri(i);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("id", i.identifier);
        payload.put("title", i.title);
        payload.put("author", i.author);
        payload.put("length", formatTrackLength(i));
        payload.put("uri", uri);

        if (i.identifier != null && i.identifier.length() == 11) {
            payload.put("thumbnail", "https://img.youtube.com/vi/" + i.identifier + "/hqdefault.jpg");
        }

        payload.putIfAbsent("thumbnail", null);

        return payload;
    }

    private static String inferUri(AudioTrackInfo i) {
        if (i.uri != null && !i.uri.isBlank()) return i.uri;
        String id = i.identifier;
        if (id == null || id.isBlank()) return null;
        if (id.length() == 11) return "https://www.youtube.com/watch?v=" + id;
        return id;
    }

    private static String timeConverter(long milliseconds) {
        long seconds = milliseconds / 1000;
        long hours = seconds / 3600;
        long minutes = (seconds % 3600) / 60;
        long secs = seconds % 60;

        String formattedMinutes = String.format("%02d", minutes);
        String formattedSeconds = String.format("%02d", secs);

        return hours > 0
            ? String.format("%02d:%s:%s", hours, formattedMinutes, formattedSeconds)
            : String.format("%s:%s", formattedMinutes, formattedSeconds);
    }

    private static String formatTrackLength(AudioTrackInfo info) {
        if (info == null || info.isStream || info.length <= 0 || info.length == Long.MAX_VALUE) {
            return "Live";
        }
        return timeConverter(info.length);
    }

    private static void handleGetSearches(Context ctx) {
        ctx.json(Map.of("searches", DatabaseManager.getInstance().selectAllSearches()));
    }

    private static void handleGetSearchById(Context ctx) {
        long id = Long.parseLong(ctx.pathParam("id"));
        Map<String, Object> result = DatabaseManager.getInstance().selectSearchWithResults(id);
        if (result != null) {
            ctx.json(result);
        } else {
            ctx.status(404).json(Map.of("error", "Sökning hittades inte"));
        }
    }

    private static void handleGetPlaybacks(Context ctx) {
        ctx.json(Map.of("playbacks", DatabaseManager.getInstance().selectAllPlaybacks()));
    }

    private static void handleGetStats(Context ctx) {
        ctx.json(DatabaseManager.getInstance().getStatistics());
    }

    private static void handleDeleteSearch(Context ctx) {
        long id = Long.parseLong(ctx.pathParam("id"));
        boolean deleted = DatabaseManager.getInstance().deleteSearch(id);
        ctx.json(Map.of("deleted", deleted));
    }

    private static void handleDeletePlayback(Context ctx) {
        long id = Long.parseLong(ctx.pathParam("id"));
        boolean deleted = DatabaseManager.getInstance().deletePlayback(id);
        ctx.json(Map.of("deleted", deleted));
    }
}
