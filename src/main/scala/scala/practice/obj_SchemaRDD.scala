package scala.practice
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

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
						dataframe_schema.printSchema()
						val sel_col=dataframe_schema.select("state", "capital","language","country")
						sel_col.show()
		}

}