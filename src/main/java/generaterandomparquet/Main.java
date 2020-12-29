package generaterandomparquet;

import ir.sahab.CustomAvroSchema;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        System.out.println();
        int pageSize = 4 * 1024 * 1024;
        Path filePath = new Path("benchmark.parquet");
        LongGenerator longGenerator = new LongGenerator(500_000_000L);
        Schema avroSchema = CustomAvroSchema.getClassSchema();

        long startTime = System.nanoTime();
        try (ParquetWriter<CustomAvroSchema> writer = AvroParquetWriter
                .<CustomAvroSchema>builder(filePath)
                .withSchema(avroSchema)
                .withRowGroupSize(pageSize*32)
                .withPageSize(pageSize)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {


            CustomAvroSchema.Builder builder = CustomAvroSchema.newBuilder();
            for (Long i : longGenerator) {
                builder.setRandomGeneratedNumber(i);
                writer.write(builder.build());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime = System.nanoTime();
        System.out.println("-----" + (endTime - startTime)/1_000_000_000 + " seconds -----------");

    }
}
