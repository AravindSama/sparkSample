package scala.practice
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class filedm1(
		state:String,
		capital:String,
		language:String,
		country:String
		)
object obj_SchemaRDD {
	def main(arg:Array[String]):Unit=
		{
				val conf = new SparkConf().setAppName("Job1").setMaster("local[*]")
						val sc= new SparkContext(conf)
						sc.setLogLevel("Error")
						val spark=SparkSession.builder.getOrCreate()
						import spark.implicits._

						val inputFile = sc.textFile("file:///C:/Spark/countries.txt")
						val inputSplit=inputFile.map(x=>x.split(","))


						val inputColumns = inputSplit
						.filter(x => x.length >= 4) // keep only rows with at least 4 columns
						.map(x => filedm1(x(0).trim, x(1).trim, x(2).trim, x(3).trim))
						val dataframe_schema= inputColumns.toDF()
						//dataframe_schema.printSchema()
						val sel_col=dataframe_schema.select("state", "capital","language","country")
						//sel_col.show()
						val fil_data = sel_col.filter("language=='English'").show()

						dataframe_schema.createOrReplaceTempView("country_table")
						spark.sql("select state,capital,language,country from country_table").show()

						val rowrdd=inputSplit.filter(x => x.length >= 4).map(x =>Row(x(0).toString().trim(),
						    x(1).toString().trim(), 
						    x(2).toString().trim(),
						    x(3).toString().trim()))
						val struct_schema=StructType(Array(
								StructField("state",StringType,true),
								StructField("capital",StringType,true),
								StructField("language",StringType,true),
								StructField("country",StringType,true)))

						val struct_df=spark.createDataFrame(rowrdd, struct_schema)
						struct_df.printSchema()
						struct_df.show()
						

		}

}