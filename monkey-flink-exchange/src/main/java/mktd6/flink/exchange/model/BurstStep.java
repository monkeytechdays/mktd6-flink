package mktd6.flink.exchange.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import mktd6.serde.kafka.BaseJsonSerde;

import java.util.Arrays;
import java.util.Optional;

public enum BurstStep {

    STEP10(1.1d, null),
    STEP9(1.2d, STEP10),
    STEP8(0.95d, STEP9),
    STEP7(0.9d, STEP8),
    STEP6(0.8d, STEP7),
    STEP5(0.7d, STEP6),
    STEP4(0.7d, STEP5),
    STEP3(0.8d, STEP4),
    STEP2(0.9d, STEP3),
    STEP1(0.95d, STEP2)
    ;

    private final double mult;
    private final Optional<BurstStep> next;

    BurstStep(double mult, BurstStep step) {
        this.mult = mult;
        this.next = Optional.ofNullable(step);
    }

    @JsonIgnore
    public double getMult() {
        return mult;
    }

    @JsonIgnore
    public Optional<BurstStep> getNext() {
        return next;
    }

    public static class Serde extends BaseJsonSerde<BurstStep> {
        public Serde() { super(BurstStep.class); }
    }

    public static void main(String[] args) {
        double d = 1;
        Double result = Arrays.stream(BurstStep.values())
                .reduce(d, (acc, step) -> acc * step.mult, (a, b) -> a * b);
        System.out.println(result);
    }
}
