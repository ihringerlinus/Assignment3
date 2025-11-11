package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class DatasetHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetHelper.class);
    public static Dataset<Row> getDataset(SparkSession sparkSession, String datasetFileName, boolean local){

        String datasetFilePath = local ? "datasets/" + datasetFileName : "hdfs://namenode:9000/datasets/" + datasetFileName;
        String hdfsDatasetPath = "hdfs://namenode:9000/datasets/";

        // Define the schema for the CSV data
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("result", DataTypes.StringType, true),
                DataTypes.createStructField("table", DataTypes.StringType, true),
                DataTypes.createStructField("_dummy", DataTypes.StringType, true),
                DataTypes.createStructField("_start", DataTypes.StringType, true),
                DataTypes.createStructField("_stop", DataTypes.StringType, true),
                DataTypes.createStructField("_time", DataTypes.StringType, true),
                DataTypes.createStructField("_value", DataTypes.DoubleType, true),
                DataTypes.createStructField("_field", DataTypes.StringType, true),
                DataTypes.createStructField("_measurement", DataTypes.StringType, true)
        });

        Dataset<Row> df = sparkSession.read()
                .option("header", true) // CSV has a header row
                .schema(schema)
                .csv(datasetFilePath);

        //LOGGER.info("Original DataFrame Schema:");
        //df.printSchema();
        //LOGGER.info("First 5 rows of original DataFrame:");
        //df.show(5);

        // --- Data Preprocessing: Cast timestamp string to actual TimestampType ---
        // This is crucial for using date/time functions (for ts with milliseconds use "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        df = df.withColumn("timestamp_parsed", to_timestamp(col("_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                .drop("_time") // Drop the original string column
                .withColumnRenamed("timestamp_parsed", "timestamp"); // Rename the parsed column

        //LOGGER.info("DataFrame Schema after timestamp parsing:");
        //df.printSchema();
        //LOGGER.info("First 5 rows after timestamp parsing:");
        //df.show(5);

        df = df
                .groupBy(col("timestamp"))
                .pivot("_field") // The column whose unique values will become new columns
                .agg(first(col("_value"))); // The aggregation function for the pivoted values

        df = df
                .withColumn("month", month(col("timestamp")))
                .withColumn("hour", hour(col("timestamp")))
                .withColumn("weekday", dayofweek(col("timestamp")))
                .withColumn("unix_timestamp", unix_timestamp(col("timestamp")));

        String[] columnNames = df.columns();
        for (String colName : columnNames) {
            // Check if the column is numeric and cast it to DoubleType
            if (df.schema().apply(colName).dataType().sameType(DataTypes.IntegerType)) {
                df = df.withColumn(colName, col(colName).cast(DataTypes.DoubleType));
            }
        }

        return df;
        //System.out.println("\nTransformed (Wide) Format DataFrame:");
        //df.show();
        //df.printSchema();
    }
}
