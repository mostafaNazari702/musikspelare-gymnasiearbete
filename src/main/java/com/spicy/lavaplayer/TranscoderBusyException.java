package com.spicy.lavaplayer;

import java.io.IOException;

public class TranscoderBusyException extends IOException {
    public TranscoderBusyException(String message) {
        super(message);
    }
}
