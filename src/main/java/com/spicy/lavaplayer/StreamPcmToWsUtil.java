package com.spicy.lavaplayer;

import com.sedmelluq.discord.lavaplayer.player.AudioPlayer;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.playback.AudioFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StreamPcmToWsUtil {
    private static final Logger logger = LoggerFactory.getLogger(StreamPcmToWsUtil.class);
    
    private static final int BOX_FTYP = 0x66747970; // 'ftyp'
    private static final int BOX_MOOV = 0x6D6F6F76; // 'moov'
    private static final int BOX_MOOF = 0x6D6F6F66; // 'moof'
    private static final int BOX_MDAT = 0x6D646174; // 'mdat'

    public static void streamToWebSocket(WebSocketSession session, TranscoderPermit permit) {
        String sessionId = session.getSessionId();
        AudioPlayer player = session.getPlayer();
        
        logger.info("Session {}: Starting WebSocket audio streaming", sessionId);
        
        try {
            runStreamingCycle(session, player);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Session {}: Streaming interrupted", sessionId);
        } catch (Exception e) {
            if (!session.isClosed()) {
                logger.error("Session {}: Streaming failed", sessionId, e);
                session.sendError("Streaming failed: " + e.getMessage());
            }
        } finally {
            permit.close();
            
            if (!session.isClosed()) {
                session.sendEnded();
            }
            
            logger.info("Session {}: WebSocket streaming complete", sessionId);
        }
    }
 
    private static void runStreamingCycle(WebSocketSession session, AudioPlayer player) throws InterruptedException {
        String sessionId = session.getSessionId();
        
        List<String> command = new ArrayList<>();
        command.add("ffmpeg");
        command.add("-threads");
        command.add("1");
        command.add("-f");
        command.add("s16le");
        command.add("-ar");
        command.add("48000");
        command.add("-ac");
        command.add("2");
        command.add("-i");
        command.add("pipe:0");
        command.add("-c:a");
        command.add("aac");
        command.add("-b:a");
        command.add("192k");
        command.add("-frag_duration");
        command.add("500000"); // en halv sekund på mikrosekunder
        command.add("-f");
        command.add("mp4");
        command.add("-movflags");
        command.add("frag_keyframe+empty_moov+default_base_moof");
        command.add("-flush_packets");
        command.add("1");
        command.add("-fflags");
        command.add("+flush_packets");
        command.add("pipe:1");
        
        logger.debug("Session {}: Starting FFmpeg", sessionId);
        
        Process ffmpeg = null;
        Thread pcmWriter = null;
        Thread outputReader = null;
        
        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(false);
            ffmpeg = pb.start();
            final Process ffmpegProcess = ffmpeg;
            
            Thread stderrConsumer = new Thread(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(ffmpegProcess.getErrorStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        logger.debug("FFmpeg: {}", line);
                    }
                } catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted()) {
                        logger.warn("Error reading FFmpeg stderr", e);
                    }
                }
            }, "ws-ffmpeg-stderr-" + sessionId);
            stderrConsumer.setDaemon(true);
            stderrConsumer.start();
            
            final BufferedOutputStream ffmpegInput = new BufferedOutputStream(ffmpegProcess.getOutputStream(), 32 * 1024);
            pcmWriter = new Thread(() -> {
                try {
                    writePcmToFfmpeg(player, session, ffmpegInput);
                } finally {
                    try {
                        ffmpegInput.close();
                    } catch (IOException ignored) {}
                }
            }, "ws-pcm-writer-" + sessionId);
            
            final BufferedInputStream ffmpegOutput = new BufferedInputStream(ffmpegProcess.getInputStream(), 64 * 1024);
            outputReader = new Thread(() -> {
                try {
                    parseFmp4AndSend(ffmpegOutput, session);
                } catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted() && !session.isClosed()) {
                        logger.error("Session {}: Error parsing fMP4 output", sessionId, e);
                        session.sendError("Streaming error: " + e.getMessage());
                    }
                }
            }, "ws-fmp4-sender-" + sessionId);
            
            pcmWriter.start();
            outputReader.start();
            
            pcmWriter.join();
            outputReader.join(5000);
            
            if (outputReader.isAlive()) {
                logger.warn("Session {}: fMP4 sender did not terminate, interrupting", sessionId);
                outputReader.interrupt();
            }
            
        } catch (Exception e) {
            if (!Thread.currentThread().isInterrupted() && !session.isClosed()) {
                logger.error("Session {}: Streaming cycle failed", sessionId, e);
            }
        } finally {
            if (pcmWriter != null && pcmWriter.isAlive()) {
                pcmWriter.interrupt();
                try {
                    pcmWriter.join(2000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            
            if (outputReader != null && outputReader.isAlive()) {
                outputReader.interrupt();
                try {
                    outputReader.join(2000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            
            if (ffmpeg != null) {
                try {
                    if (!ffmpeg.waitFor(3, TimeUnit.SECONDS)) {
                        logger.debug("Session {}: FFmpeg did not exit, forcing shutdown", sessionId);
                        ffmpeg.destroyForcibly();
                        ffmpeg.waitFor(1, TimeUnit.SECONDS);
                    }
                } catch (InterruptedException ignored) {
                    ffmpeg.destroyForcibly();
                    Thread.currentThread().interrupt();
                }
            }
            
            logger.debug("Session {}: FFmpeg cleanup complete", sessionId);
        }
    }

    private static void writePcmToFfmpeg(
        AudioPlayer player,
        WebSocketSession session,
        OutputStream ffmpegInput
    ) {
        String sessionId = session.getSessionId();
        
        try {
            logger.debug("Session {}: Starting PCM extraction", sessionId);
            long startTime = System.currentTimeMillis();
            
            int initRetries = 0;
            final int maxInitRetries = 100; // 5 seconds max
            
            while (initRetries < maxInitRetries) {
                if (Thread.currentThread().isInterrupted() || session.isClosed()) {
                    logger.debug("Session {}: Interrupted during init wait", sessionId);
                    return;
                }
                
                AudioTrack track = player.getPlayingTrack();
                if (track != null) {
                    AudioFrame testFrame = player.provide();
                    if (testFrame != null) {
                        ffmpegInput.write(testFrame.getData());
                        logger.info("Session {}: First PCM frame at {}ms after {}ms init", 
                            sessionId, track.getPosition(), initRetries * 50);
                        break;
                    }
                }
                
                Thread.sleep(50);
                initRetries++;
            }
            
            if (initRetries >= maxInitRetries) {
                logger.warn("Session {}: Timeout waiting for first audio frame", sessionId);
                session.sendError("Timeout waiting for audio");
                return;
            }
            
            AudioFrame frame;
            int frameCount = 1;
            long totalBytesWritten = 0;
            long lastPositionUpdate = System.currentTimeMillis();
            
            while (true) {
                if (Thread.currentThread().isInterrupted() || session.isClosed()) {
                    logger.debug("Session {}: PCM writer interrupted", sessionId);
                    break;
                }
                
                WebSocketSession.State state = session.getState();
                if (state == WebSocketSession.State.STOPPED) {
                    logger.debug("Session {}: Stopped, halting PCM extraction", sessionId);
                    break;
                }
                
                if (state == WebSocketSession.State.PAUSED) {
                    Thread.sleep(50);
                    continue;
                }
                
                frame = player.provide();
                
                if (frame != null) {
                    byte[] pcmData = frame.getData();
                    ffmpegInput.write(pcmData);
                    frameCount++;
                    totalBytesWritten += pcmData.length;
                    
                    long now = System.currentTimeMillis();
                    if (now - lastPositionUpdate >= 500) {
                        AudioTrack track = player.getPlayingTrack();
                        if (track != null) {
                            session.sendPosition(track.getPosition());
                        }
                        lastPositionUpdate = now;
                    }
                    
                    if (frameCount % 500 == 0) {
                        logger.debug("Session {}: {} frames, {} KB written",
                            sessionId, frameCount, totalBytesWritten / 1024);
                    }
                } else {
                    // Check if track ended
                    AudioTrack currentTrack = player.getPlayingTrack();
                    if (currentTrack == null) {
                        Thread.sleep(50);
                        currentTrack = player.getPlayingTrack();
                        if (currentTrack == null) {
                            logger.debug("Session {}: Track ended after {} frames", 
                                sessionId, frameCount);
                            break;
                        }
                    }
                    Thread.sleep(10);
                }
            }
            
            ffmpegInput.flush();
            
            long elapsed = System.currentTimeMillis() - startTime;
            logger.info("Session {}: PCM extraction complete. Frames: {}, Bytes: {} KB, Duration: {}s",
                sessionId, frameCount, totalBytesWritten / 1024, elapsed / 1000);
            
        } catch (IOException e) {
            if (!Thread.currentThread().isInterrupted() && !session.isClosed()) {
                logger.error("Session {}: Error writing PCM to FFmpeg", sessionId, e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Session {}: PCM writer interrupted", sessionId);
        }
    }

    private static void parseFmp4AndSend(
        InputStream input,
        WebSocketSession session
    ) throws IOException {
        String sessionId = session.getSessionId();
        
        ByteArrayOutputStream initBuffer = new ByteArrayOutputStream();
        boolean initSent = false;
        int chunkCount = 0;
        
        byte[] headerBuf = new byte[8];
        
        while (!Thread.currentThread().isInterrupted() && !session.isClosed()) {
            int read = readFully(input, headerBuf, 0, 8);
            if (read < 8) {
                if (read > 0) {
                    logger.debug("Session {}: Incomplete box header at end of stream", sessionId);
                }
                break;
            }
            
            ByteBuffer header = ByteBuffer.wrap(headerBuf).order(ByteOrder.BIG_ENDIAN);
            long boxSize = Integer.toUnsignedLong(header.getInt(0));
            int boxType = header.getInt(4);
            long dataSize;
            byte[] extendedHeader = null;
            if (boxSize == 1) {
                extendedHeader = new byte[8];
                if (readFully(input, extendedHeader, 0, 8) < 8) {
                    logger.warn("Session {}: Incomplete extended size", sessionId);
                    break;
                }
                boxSize = ByteBuffer.wrap(extendedHeader).order(ByteOrder.BIG_ENDIAN).getLong(0);
                dataSize = boxSize - 16;
            } else if (boxSize == 0) {
                ByteArrayOutputStream remaining = new ByteArrayOutputStream();
                byte[] chunk = new byte[8192];
                int r;
                while ((r = input.read(chunk)) != -1) {
                    remaining.write(chunk, 0, r);
                }
                byte[] boxData = remaining.toByteArray();
                                ByteArrayOutputStream fullBox = new ByteArrayOutputStream();
                fullBox.write(headerBuf);
                fullBox.write(boxData);
                session.sendAudioData(fullBox.toByteArray());
                break;
            } else {
                dataSize = boxSize - 8;
            }
                        if (dataSize > 100_000_000) {
                logger.error("Session {}: Box size too large: {}", sessionId, boxSize);
                break;
            }
            
            byte[] boxData = new byte[(int) dataSize];
            if (readFully(input, boxData, 0, boxData.length) < boxData.length) {
                logger.debug("Session {}: Incomplete box data", sessionId);
                break;
            }
            
            ByteArrayOutputStream fullBox = new ByteArrayOutputStream();
            fullBox.write(headerBuf);
            if (extendedHeader != null) {
                fullBox.write(extendedHeader);
            }
            fullBox.write(boxData);
            byte[] boxBytes = fullBox.toByteArray();
            
            if (boxType == BOX_FTYP || boxType == BOX_MOOV) {
                initBuffer.write(boxBytes);
                
                if (boxType == BOX_MOOV) {
                    
                    session.sendInitSegment(initBuffer.toByteArray());
                    initSent = true;
                    logger.debug("Session {}: Init segment sent ({} bytes)", 
                        sessionId, initBuffer.size());
                }
            } else if (boxType == BOX_MOOF || boxType == BOX_MDAT) {
                if (boxType == BOX_MDAT) {
                    session.sendAudioData(boxBytes);
                    chunkCount++;
                    
                    if (chunkCount % 10 == 0) {
                        logger.debug("Session {}: Sent {} audio chunks", sessionId, chunkCount);
                    }
                } else {                    session.sendAudioData(boxBytes);
                }
            } else {
                if (initSent) {
                    session.sendAudioData(boxBytes);
                } else {
                    initBuffer.write(boxBytes);
                }
            }
        }
        
        logger.info("Session {}: fMP4 parsing complete, {} chunks sent", sessionId, chunkCount);
    }
    
    private static int readFully(InputStream in, byte[] buf, int off, int len) throws IOException {
        int totalRead = 0;
        while (totalRead < len) {
            int read = in.read(buf, off + totalRead, len - totalRead);
            if (read < 0) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }
}
