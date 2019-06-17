package com.realtime.flink;


public class TimeoutEvent extends Event{

    public TimeoutEvent() {
    }

    public TimeoutEvent(Long id, String type) {
        super(id, type);
    }
}
