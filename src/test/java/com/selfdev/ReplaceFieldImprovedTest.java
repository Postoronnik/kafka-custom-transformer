package com.selfdev;

import com.selfdev.ReplaceFieldImproved;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReplaceFieldImprovedTest {
    private final ReplaceFieldImproved<SinkRecord> xform = new ReplaceFieldImproved.Value<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void tombstoneSchemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("renames", "abc:xyz,foo:bar,ssd:new_ssd");

        xform.configure(props);

        final Schema internalSchema = SchemaBuilder.struct()
                .field("ssd", Schema.STRING_SCHEMA)
                .build();

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("internalSchema", internalSchema)
                .build();

//        System.out.println("______________");
//
//        schema.fields().forEach(System.out::println);
//        schema.fields().forEach(field -> {
//            System.out.println("______________");
//
//            field.schema().fields()
//                    .stream()
//                    .filter(field1 -> {
//                        return field.schema().type().getName().equals("STRUCT");
//                    })
//                    .forEach(System.out::println);
//
//            System.out.println("______________");
//        });
//
//        System.out.println("______________");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(schema, transformedRecord.valueSchema());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void schemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals(42, updatedValue.get("xyz"));
        assertEquals(true, updatedValue.get("bar"));
    }

    @Test
    public void withSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("renames", "abc:xyz, foo:bar, internal_schema:internalSchema, ssd:new_ssd, internal_schema_1:internalSchema1");

        xform.configure(props);

        final Schema internalSchema1 = SchemaBuilder.struct()
                .field("ssd", Schema.STRING_SCHEMA)
                .build();

        final Schema internalSchema2 = SchemaBuilder.struct()
                .field("ssd", Schema.STRING_SCHEMA)
                .build();

        final Schema internalSchema3 = SchemaBuilder.struct()
                .field("ssd", Schema.STRING_SCHEMA)
                .field("internal_schema", internalSchema2)
                .build();

        final Schema schema = SchemaBuilder.struct()
                .field("internal_schema", internalSchema1)
                .field("internal_schema_1", internalSchema3)
                .build();

        final Struct internalValue1 = new Struct(internalSchema1);
        internalValue1.put("ssd", "test");

        final Struct internalValue2 = new Struct(internalSchema2);
        internalValue2.put("ssd", "test1");

        final Struct internalValue3 = new Struct(internalSchema3);
        internalValue3.put("ssd", "test2");
        internalValue3.put("internal_schema", internalValue2);

        final Struct value = new Struct(schema);
        value.put("internal_schema", internalValue1);
        value.put("internal_schema_1", internalValue3);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();


        assertEquals("test",
                updatedValue
                        .getStruct("internalSchema")
                        .getString("new_ssd")
        );

        assertEquals("test1",
                updatedValue
                        .getStruct("internalSchema1")
                        .getStruct("internalSchema")
                        .getString("new_ssd")
        );

        assertEquals("test2",
                updatedValue
                        .getStruct("internalSchema1")
                        .getString("new_ssd")
        );
    }

    @Test
    public void testIncludeBackwardsCompatibility() {
        final Map<String, String> props = new HashMap<>();
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testExcludeBackwardsCompatibility() {
        final Map<String, String> props = new HashMap<>();
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals(42, updatedValue.get("xyz"));
        assertEquals(true, updatedValue.get("bar"));
        assertEquals("etc", updatedValue.get("etc"));
    }
}