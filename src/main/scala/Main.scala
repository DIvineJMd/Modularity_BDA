import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.Random

object GitHubCommunityDetection {
  def main(args: Array[String]): Unit = {
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("GitHub Community Detection")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load edge data
    val edgesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/musae_git_edges.csv")
      .select("id_1", "id_2") // Adjust column names as needed
      .na.drop()

    // Create edges RDD
    val edgesRDD: RDD[Edge[Double]] = edgesDF.rdd.flatMap(row => {
      val srcId = row.getAs[Int]("id_1").toLong
      val dstId = row.getAs[Int]("id_2").toLong
      if (srcId != dstId) Some(Edge(srcId, dstId, 1.0)) else None
    })

    // Create graph
    val graph = Graph.fromEdges(edgesRDD, 0)
    println(s"Graph has ${graph.vertices.count()} vertices and ${graph.edges.count()} edges.")

    // Initialize vertex attributes
    val numCommunities = 10
    val initialVertexRDD: RDD[(VertexId, Array[Double])] = graph.vertices.mapValues(_ =>
      Array.fill(numCommunities)(Random.nextDouble())
    )
    val graphWithFeatures = Graph(initialVertexRDD, graph.edges)

    // Detect communities
    val communityGraph = detectCommunities(graphWithFeatures, numCommunities)

    // Display sample community assignments
    communityGraph.vertices.take(5).foreach(println)

    // Calculate modularity
    val modularityScore = calculateModularity(communityGraph)
    println(s"Modularity Score: $modularityScore")

    spark.stop()
  }

  def detectCommunities(graph: Graph[Array[Double], Double], numCommunities: Int): Graph[Int, Double] = {
    var currentGraph = graph
    val maxIterations = 10

    for (_ <- 1 to maxIterations) {
      val messages = currentGraph.aggregateMessages[Array[Double]](
        triplet => {
          triplet.sendToDst(triplet.srcAttr)
          triplet.sendToSrc(triplet.dstAttr)
        },
        (msg1, msg2) => msg1.zip(msg2).map { case (a, b) => math.max(a, b) } // Combine messages
      )

      currentGraph = currentGraph.outerJoinVertices(messages) {
        case (_, oldAttr, Some(newAttr)) =>
          oldAttr.zip(newAttr).map { case (a, b) => (a + b) / 2 }
        case (_, oldAttr, None) => oldAttr
      }
    }

    currentGraph.mapVertices { case (_, attr) =>
      attr.indexOf(attr.max)
    }
  }

  def calculateModularity(graph: Graph[Int, Double]): Double = {
    val m = graph.edges.map(_.attr).sum()

    val communityEdges = graph.triplets.map { triplet =>
      val sameCommunity = if (triplet.srcAttr == triplet.dstAttr) 1.0 else 0.0
      sameCommunity * triplet.attr
    }.sum()

    communityEdges / m
  }
}
