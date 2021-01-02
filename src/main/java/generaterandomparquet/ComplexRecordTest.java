package generaterandomparquet;

import ir.sahab.CustomAvroSchema;
import ir.sahab.complex.ComplexRecord;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComplexRecordTest {
    public static void main(String[] args) {
        System.out.println();
        int pageSize = 4 * 1024 * 1024;
        Path filePath = new Path("complex/benchmark-complex.parquet");
        LongGenerator longGenerator = new LongGenerator(50_000_000L);
        Schema avroSchema = ComplexRecord.getClassSchema();


        ComplexRecord.Builder builder = ComplexRecord.newBuilder();
        builder.setAge(12003);
        builder.setFirstName("hatef");
        builder.setHeight(1234.44545f);
        List<String> hobbies = new ArrayList<>();
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        hobbies.add("bodybuilding");
        builder.setHobbies(hobbies);

        Map<String, String> mappy = new HashMap<>();
        mappy.put("ali", "hatef");
        mappy.put("ali12", "hatef12");
        mappy.put("ali13", "hatef13");
        mappy.put("ali14", "hatef14");
        mappy.put("ali15", "hatef15");
        builder.setMappy(mappy);
        builder.setLastName("alipour");
        builder.setWeight(121213.45454f);

        long startTime = System.nanoTime();

        try (ParquetWriter<ComplexRecord> writer = AvroParquetWriter
                .<ComplexRecord>builder(filePath)
                .withSchema(avroSchema)
                .withRowGroupSize(pageSize*32)
                .withPageSize(pageSize)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {


            for (Long i : longGenerator) {

                ComplexRecord x = builder.build();
                x.getAge();
                writer.write(builder.build());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("----- result for avro library version 1.11.0: " + (endTime - startTime)/1_000_000_000 + " seconds -----------");

    }
}
