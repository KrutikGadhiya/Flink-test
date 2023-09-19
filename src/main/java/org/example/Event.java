package org.example;

import lombok.*;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class Event {
    private long ts;
    private String conf;
    private String id;
    private int val;

    public static TypeSerializer<Event> createTypeSerializer() {
        TypeInformation<Event> typeInformation = TypeExtractor.createTypeInfo(Event.class);

        return typeInformation.createSerializer(new ExecutionConfig());
    }
}
