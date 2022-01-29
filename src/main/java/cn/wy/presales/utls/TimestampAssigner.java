package cn.wy.presales.utls;

import cn.wy.presales.model.StockEvent;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;


public class TimestampAssigner implements AssignerWithPunctuatedWatermarks<StockEvent> {

    @Override
    public long extractTimestamp(StockEvent element, long previousElementTimestamp) {
        return element.getTimestamp();
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(StockEvent event, long l) {
        return new Watermark(l);
    }
}