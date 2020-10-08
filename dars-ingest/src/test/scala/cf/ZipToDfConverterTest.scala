package cf

import org.scalatest.Matchers

class ZipToDfConverterTest extends org.scalatest.FunSuite
  with Matchers
  with SparkSessionWrapper {

  test("test zip"){
    val path = "./hes_zips/NIC243790_HES_AE_201599.zip"
    val dir = "./hes_zips"
    val filename = "NIC243790_HES_AE_201599.zip"
    val zipPath = s"${dir}/$filename"
    val minPartitions = 10
    val separator = '|'
    val df = ZipToDfConverter.exportZipToDf(filename, dir, 100)(spark)
    df.show(false)
    df.count shouldBe 99 // interesting that count takes minutes but show very quick.
    df.columns.length should be > 150
  }

}
