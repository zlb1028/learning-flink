package com.realtime.flink;

public class NormalEvent extends Event {
    public NormalEvent() {
    }

    public NormalEvent(Long id, String type) {
        super(id, type);
    }
}
