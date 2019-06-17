package com.realtime.flink;


public class TempEvent extends Event{
    private Long value;

    public TempEvent(Long value) {
        this.value = value;
    }

    public TempEvent(Long id, String type, Long value) {
        super(id, type);
        this.value = value;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }
}
