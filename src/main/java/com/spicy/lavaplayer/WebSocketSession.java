package com.spicy.lavaplayer;

import com.sedmelluq.discord.lavaplayer.player.AudioPlayer;
import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import io.javalin.websocket.WsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class WebSocketSession {
    private static final Logger logger = LoggerFactory.getLogger(WebSocketSession.class);
    
    public enum State {
        IDLE,
        LOADING,
        PLAYING,
        PAUSED,
        STOPPED
    }
    
    private final String sessionId;
    private final WsContext wsContext;
    private final AudioPlayerManager playerManager;
    private final AudioPlayer player;
    
    private final AtomicReference<State> state = new AtomicReference<>(State.IDLE);
    private final AtomicReference<AudioTrack> currentTrack = new AtomicReference<>();
    private final AtomicReference<Thread> streamingThread = new AtomicReference<>();
    private final AtomicReference<TranscoderPermit> permit = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong lastPositionMs = new AtomicLong(0);
    
    public WebSocketSession(String sessionId, WsContext wsContext, AudioPlayerManager playerManager) {
        this.sessionId = sessionId;
        this.wsContext = wsContext;
        this.playerManager = playerManager;
        this.player = playerManager.createPlayer();
        
        logger.info("WebSocket session created: {}", sessionId);
    }
    
    public String getSessionId() {
        return sessionId;
    }
    
    public AudioPlayer getPlayer() {
        return player;
    }
    
    public AudioPlayerManager getPlayerManager() {
        return playerManager;
    }
    
    public State getState() {
        return state.get();
    }
    
    public void setState(State newState) {
        State old = state.getAndSet(newState);
        if (old != newState) {
            logger.debug("Session {}: state {} -> {}", sessionId, old, newState);
        }
    }
    
    public boolean isClosed() {
        return closed.get();
    }
    
    public AudioTrack getCurrentTrack() {
        return currentTrack.get();
    }
    
    public void setCurrentTrack(AudioTrack track) {
        currentTrack.set(track);
    }
    
    public void setStreamingThread(Thread thread) {
        streamingThread.set(thread);
    }
    
    public Thread getStreamingThread() {
        return streamingThread.get();
    }
    
    public void setPermit(TranscoderPermit p) {
        permit.set(p);
    }
    
    public TranscoderPermit getPermit() {
        return permit.get();
    }
    
    public long getLastPositionMs() {
        return lastPositionMs.get();
    }
    
    public void updatePosition(long positionMs) {
        lastPositionMs.set(positionMs);
    }
    
    /**
     * Send a JSON message to the client.
     */
    public void sendJson(Map<String, Object> message) {
        if (closed.get() || wsContext.session == null || !wsContext.session.isOpen()) {
            return;
        }
        try {
            wsContext.send(message);
        } catch (Exception e) {
            logger.warn("Session {}: Failed to send JSON", sessionId, e);
        }
    }
    
    /**
     * Send a binary message (fMP4 data) to the client.
     */
    public void sendBinary(byte[] data) {
        if (closed.get() || wsContext.session == null || !wsContext.session.isOpen()) {
            return;
        }
        try {
            wsContext.send(ByteBuffer.wrap(data));
        } catch (Exception e) {
            logger.warn("Session {}: Failed to send binary", sessionId, e);
        }
    }
    
    /**
     * Send the initialization segment (for MSE SourceBuffer).
     */
    public void sendInitSegment(byte[] initData) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "init");
        msg.put("data", Base64.getEncoder().encodeToString(initData));
        sendJson(msg);
    }
    
    /**
     * Send an audio segment.
     * For efficiency, we send as binary frame.
     */
    public void sendAudioData(byte[] audioData) {
        sendBinary(audioData);
    }
    
    /**
     * Send track metadata to the client.
     */
    public void sendMetadata(AudioTrackInfo info) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "metadata");
        msg.put("title", info.title);
        msg.put("author", info.author);
        msg.put("duration", info.length);
        msg.put("isStream", info.isStream);
        msg.put("uri", info.uri);
        
        // Thumbnail for YouTube
        if (info.identifier != null && info.identifier.length() == 11) {
            msg.put("thumbnail", "https://img.youtube.com/vi/" + info.identifier + "/hqdefault.jpg");
        }
        
        sendJson(msg);
    }
    
    /**
     * Send position update to the client.
     */
    public void sendPosition(long positionMs) {
        updatePosition(positionMs);
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "position");
        msg.put("position", positionMs);
        sendJson(msg);
    }
    
    /**
     * Notify client that playback has ended.
     */
    public void sendEnded() {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "ended");
        sendJson(msg);
    }
    
    /**
     * Send error message to the client.
     */
    public void sendError(String message) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "error");
        msg.put("message", message);
        sendJson(msg);
    }
    
    /**
     * Send state update to the client.
     */
    public void sendState(String stateStr) {
        Map<String, Object> msg = new LinkedHashMap<>();
        msg.put("type", "state");
        msg.put("state", stateStr);
        sendJson(msg);
    }
    
    /**
     * Pause playback.
     */
    public void pause() {
        if (state.get() == State.PLAYING) {
            player.setPaused(true);
            setState(State.PAUSED);
            sendState("paused");
        }
    }
    
    /**
     * Resume playback.
     */
    public void resume() {
        if (state.get() == State.PAUSED) {
            player.setPaused(false);
            setState(State.PLAYING);
            sendState("playing");
        }
    }
    
    /**
     * Stop playback and clean up streaming.
     */
    public void stopPlayback() {
        setState(State.STOPPED);
        
        // Interrupt streaming thread
        Thread streamThread = streamingThread.getAndSet(null);
        if (streamThread != null && streamThread.isAlive()) {
            streamThread.interrupt();
            try {
                streamThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Stop the player
        player.stopTrack();
        currentTrack.set(null);
        
        // Release permit
        TranscoderPermit p = permit.getAndSet(null);
        if (p != null) {
            p.close();
        }
        
        sendState("stopped");
    }
    
    /**
     * Close the session and release all resources.
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        
        logger.info("Session {}: Closing", sessionId);
        
        stopPlayback();
        
        // Destroy the player
        player.destroy();
        
        // Close WebSocket if still open
        try {
            if (wsContext.session != null && wsContext.session.isOpen()) {
                wsContext.closeSession();
            }
        } catch (Exception e) {
            logger.debug("Session {}: Error closing WebSocket", sessionId, e);
        }
    }
}
