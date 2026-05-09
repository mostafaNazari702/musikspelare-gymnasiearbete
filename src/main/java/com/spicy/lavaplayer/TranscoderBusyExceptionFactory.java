package com.spicy.lavaplayer;

public interface TranscoderBusyExceptionFactory {
    TranscoderBusyException create(String message);
}
