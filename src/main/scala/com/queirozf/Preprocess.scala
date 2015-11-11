package com.queirozf

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{SQLContext, Row, Column}
import org.apache.spark.sql.functions.{udf, col, max, min, concat}
import org.apache.spark.ml.feature._

import org.joda.time.{DateTime, LocalTime}

import utils.UDF

/**
 * Script to clean our dataset.
 *
 * input dataset shape:
 *
 * |-- asin: string (nullable = true)
 * |-- helpful: array (nullable = true)
 * |    |-- element: long (containsNull = true)
 * |-- overall: double (nullable = true)
 * |-- reviewText: string (nullable = true)
 * |-- reviewTime: string (nullable = true)
 * |-- reviewerID: string (nullable = true)
 * |-- reviewerName: string (nullable = true)
 * |-- summary: string (nullable = true)
 * |-- unixReviewTime: long (nullable = true)
 *
 *
 */
object Preprocess {

  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Please set arguments for <s3_input_dir> <s3_output_dir>")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1) // in case we want to output the preprocessed data

    val cnf = new SparkConf()
      .setAppName("Cleaning and Featurizing Amazon Review Data")

    val sc = new SparkContext(cnf)

    val sqlContext = new SQLContext(sc)

    // initializing the dataframe from json file
    val reviewsDF = sqlContext.read.json(inputDir)

    // schema is inferred automatically
    val schema = reviewsDF.schema

    // transform dataframe into RDD so that we can call filter
    // to remove any rows with Null values
    val cleanRDD = reviewsDF.rdd.filter { row: Row => !row.anyNull && row.getSeq[Option[Long]](row.fieldIndex("helpful")).forall( num => num.isDefined) }

    // then recreate the dataframe
    val df = sqlContext.createDataFrame(cleanRDD, schema)

    // add a column containing the length of the review text
    val df1 = df.withColumn("reviewLen", UDF.len(df("reviewText")))

    // add a column containing the length of the summary
    val df2 = df1.withColumn("summaryLen", UDF.len(df1("summary")))

    // merge the two text attributes into a single one
    val df3 = df2.withColumn("text", concat(df2("reviewText"), df2("summary"))).drop("reviewText").drop("summary")

    // lowercase
    val df4 = df3.withColumn("textlc",UDF.toLower(df3("text"))).drop("text")

    val tokenizer = new RegexTokenizer()
      .setInputCol("textlc")
      .setOutputCol("tokens")
      .setPattern("\\s+|,|\\\\.|-")

    val df5 = tokenizer.transform(df4).drop("textlc")

    // transform tokens into frequency vectors using the hashing trick
    val hashingTF = new HashingTF().setInputCol("tokens").setOutputCol("freqs")

    val df6 = hashingTF.transform(df5).drop("tokens")

    val df7 = df6.withColumn("AM",UDF.timestampIsAM(df6("unixReviewTime")))

    val df8 = df7.withColumn("PM",UDF.timestampIsPM(df7("unixReviewTime")))

    val df9 = df8.withColumn("weekday",UDF.timestampIsWeekDay(df8("unixReviewTime")))

    val df10 = df9.withColumn("weekend",UDF.timestampIsWeekend(df9("unixReviewTime")))

    val df11 = df10.withColumn("ratioHelpful",UDF.getRatio(df10("helpful")))

    // need to assemble values into a vector before i can normalize them
    val assembler = new VectorAssembler()
      .setInputCols(Array("ratioHelpful","overall"))
      .setOutputCol("vec")

    val df12 = assembler.transform(df11)

    val scalerModel = new MinMaxScaler()
      .setInputCol("vec")
      .setOutputCol("vecNorm")
      .fit(df12)

    val df13 = scalerModel.transform(df12)

    val ex = df13.take(1) // just to force an action

    // at this point we would either write the resulting df to disk/S3/HDFS or start performing the actual
    // machine learning task

    sc.stop()

  }

}
