package cn.wy.presales.model;

import com.google.gson.*;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.time.Instant;


public abstract class Event {

    private static final String TYPE_FIELD = "type";


    private static final Gson gson = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> Instant.parse(json.getAsString()))
            .create();

    public static Event parseEvent(byte[] event) {
        //parse the event payload and remove the type attribute
        JsonReader jsonReader =  new JsonReader(new InputStreamReader(new ByteArrayInputStream(event)));
        JsonElement jsonElement = Streams.parse(jsonReader);
        //JsonElement labelJsonElement = jsonElement.getAsJsonObject().remove(TYPE_FIELD);


        return gson.fromJson(jsonElement, StockEvent.class);
    }

    /**
     * @return timestamp in epoch millies
     */
    public abstract long getTimestamp();
}