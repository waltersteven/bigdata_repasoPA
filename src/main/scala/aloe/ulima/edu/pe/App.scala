package aloe.ulima.edu.pe
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author ${user.name}
 */
object App {

  /*def filtrado(){
    val conf = new SparkConf().setAppName("fifa").setMaster("local")
    val sc = new SparkContext(conf)

    val dataset = sc.textFile("data/fifaData.csv")
    val mapa = dataset.map( x => x.split(","))

    val mapaFiltradoEdadYPosicion = mapa.filter( x => x(14).toInt <= 23 )
                                    //.filter( x => x(15) != "" )

    //Elijiendo GoalKeeper
    val mapaArqueros = mapaFiltradoEdadYPosicion.filter( x => x(15) == "GK" )
                        .filter( x => x(0) != "" && x(14) != "" && x(15) != "" && x(25) != "" && x(48) != "" && x(49) != "" && x(50) != "" && x(51) != "" && x(52) != "")
                        .map( x => Array( x(0), x(14), x(15), x(25), x(48), x(49), x(50), x(51), x(52)))
                        .map(x => (x(0), ( x(1), ( x(2), x(3).toInt + x(4).toInt + x(5).toInt + x(6).toInt + x(7).toInt + x(8).toInt) ) ) )


    //mapaArqueros.map(x => x(0) + "," + x(1) + ","  + x(2) + ","  + x(3) + ","  + x(4) + ","  + x(5) + ","  + x(6) + ","  + x(7) + ","  + x(8))// + "," + x(49) + "," + x(50) + "," + x(51) + "," + x(52)).saveAsTextFile("data/resultado")
    mapaArqueros.saveAsTextFile("data/resultado3")
    println("busqueda finalizada")
  }


  def otros(){
    //lista de numeros
    val listNumbers = Array(1,6,9,12,15)
    //Lo convertimos en RDD
    val listDistributed = sc.parallelize(listNumbers)
    //listDistributed.saveAsTextFile("data/listResult")

    //val mapa = distributedFile.map( x => (x,1) )
    val mapa = distributedFile.map( x => x.split(",") )
    mapa.filter( x => x(8) != "" )
    .sortBy( x => x(8).toFloat, ascending=true )
    .map( x => (x(11),x(8)) )
    .saveAsTextFile("data/mapaResult") //necesita un map con tupla

    val chooseOne = mapa.filter( x => x(8) != "" )
      .sortBy( x => x(8).toFloat, ascending=true )
      .map( x => ( x(11), x(8) ) )
      .take(1)

    sc.parallelize(chooseOne).saveAsTextFile("data/mapaResultTaken")
  }
  */

  def ordenar(){
    val conf = new SparkConf().setAppName("PracticandoParcial").setMaster("local")
    val sc = new SparkContext(conf)

    val distributedFile = sc.textFile("data/movie_metadata.csv")
    val mapa = distributedFile.map( x => x.split(",") )

    val rddFiltered = mapa.filter( x => x(8) != "" )

    val rddFinal = rddFiltered.sortBy( x => x(8).toFloat, ascending = true )
      .map( x => ( x(11), x(8) ) )

    rddFinal.saveAsTextFile("data/sortedResult")
  }

  def busqueda(){
    val conf = new SparkConf().setAppName("PracticandoParcial").setMaster("local")
    val sc = new SparkContext(conf)

    val distributedFile = sc.textFile("data/movie_metadata.csv")
    val mapa = distributedFile.map( x => x.split(",") )

    val searchInRdd = mapa.filter( x => x(16).contains("love") )
    .map( x => ( x(11), x(16) ) ) //despues de un filter siempre va un map

    searchInRdd.saveAsTextFile("data/searchResult")
  }

  def filtrado(){
    val conf = new SparkConf().setAppName("PracticandoParcial").setMaster("local")
    val sc = new SparkContext(conf)

    val distributedFile = sc.textFile("data/movie_metadata.csv")
    val mapa = distributedFile.map( x => x.split(",") )

    val filterEmptyValues = mapa.filter( x => x(0) != "" )
    val filterColorValues = mapa.filter( x => x(0) == "Color" )

    val numRegistros = mapa.count()
    println(numRegistros)
  }

  def demo(){
    val conf = new SparkConf().setAppName("PracticandoParcial").setMaster("local")
    val sc = new SparkContext(conf)

    val distributedFile = sc.textFile("data/FullData.csv")

    val mapa = distributedFile.map( x => x.split(",") )

    val mapKeyValue = mapa.map( x => ( x(1), ( x(14).toInt, 1 ) ) )

    val rddReduced = mapKeyValue.reduceByKey( (x,y) => ( x._1 + y._1, x._2 + y._2 ) ) //busca en el todos los indices del RDD los keys iguales y le aniade la funcion de suma de puntaje y suma de veces

    val rddReducedFinal = rddReduced.map( x => ( x._1, 1.0*x._2._1/x._2._2 ) )

    rddReducedFinal.saveAsTextFile("data/RddReducedFinal")

  }

  def operaciones(){
    val result = (1).+( (2).*( (30)./(5) ) )
    println(result)
  }

  def mayusculas(texto: String): String = {
    texto.toUpperCase() //da igual el return
  }

  def capitalize(texto: String): String = {
    texto.substring(0,1).toUpperCase + texto.substring(1).toLowerCase //da igual el return
  }

  def formatear(funcion: String => String, nombre: String){ //la funcion acepta un argumento tipo String y retorna un valor String
    println(funcion(nombre))
  }

  def unaVezPorSegundo(callback: () => Unit){
    while(true){
      callback()
      Thread sleep 2000
    }
  }

  def main(args : Array[String]) {
    println( "Hello World!" )

    //ordenar()
    //busqueda()
    //filtrado()
    //demo()
    //operaciones //operaciones()
    //formatear(mayusculas, "walter")
    unaVezPorSegundo( () => println("Accion por cada 2 segundos"))

  }

}
