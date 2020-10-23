package cf

import org.scalatest.Matchers

class ZipToDfBatchedTest extends org.scalatest.FunSuite
  with Matchers
  with SparkSessionWrapper {

  test("test zip fast"){
    val path = "./hes_zips/test_ae.zip"
//    val path = "./hes_zips/NIC243790_HES_AE_201599.zip"
//    val dir = "./hes_zips"
//    val filename = "NIC243790_HES_AE_201599.zip"
//    val zipPath = s"${dir}/$filename"

    var start = System.currentTimeMillis()
    val limit = 5
    val df = ZipToDfBatched.get_top_n_rows(path, limit,2, true)(spark)

    df.show(false)
    println(s"time taken to transform zip to df and show: ${System.currentTimeMillis()-start} millis")

    df.groupBy(df("admi_partition")).count.show(false)
    df.show(false)
    println(s"time taken to transform zip to parq and re-read for analysis: ${System.currentTimeMillis()-start} millis")
    df.count shouldBe limit // interesting that count takes minutes but show very quick.
    df.columns.length should be > 150

  }

}
