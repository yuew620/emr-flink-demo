package cn.wy.presales.utls;

import cn.wy.presales.model.Event;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class EventDeserializationSchema extends AbstractDeserializationSchema<Event> {


    @Override
    public Event deserialize(byte[] bytes) {
        try {
            Event event = Event.parseEvent(bytes);

            return event;
        } catch (Exception e) {

            return null;
        }
    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeExtractor.getForClass(Event.class);
    }

}

