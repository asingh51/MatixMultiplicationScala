
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

@SerialVersionUID(123L)
case class M_Matrix ( i: Long, j: Long, v: Double )
extends Serializable {}

@SerialVersionUID(123L)
case class N_Matrix ( j: Long, k: Long, w: Double )
extends Serializable {}

object Multiply {
  def main(args: Array[ String ]) {
   
    val conf = new SparkConf().setAppName("Multiply")
    val sc = new SparkContext(conf)
    val M_ = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                M_Matrix(a(0).toInt,a(1).toInt,a(2).toDouble) } )
    val N_ = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                N_Matrix(a(0).toInt,a(1).toInt,a(2).toDouble) } )
    val res = M_.map( M_ => (M_.j, (M_.i, M_.v))).join(N_.map(N_ => (N_.j, (N_.k, N_.w))))
    .map{ case(j, ((i,v),(k,w))) => ((i,k), v * w)}
    .reduceByKey(_ + _)
    .sortByKey()
    .map({ case ((i,k), sum) => (i+" "+k+" "+sum)} )

    res.saveAsTextFile(args(2))
    sc.stop()

  }
}