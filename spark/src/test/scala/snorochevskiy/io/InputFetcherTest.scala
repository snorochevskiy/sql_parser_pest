package snorochevskiy.io

import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class InputFetcherTest extends AnyWordSpec with Matchers {

  "input fetcher" must {
    "load from file" in {
      val res = InputFetcher.fetchJobInput(getClass.getClassLoader.getResource("test_inputs/test_input1.xml").toString)
      res.withBlocks mustBe List(
        WithBlock("my_db", "country", "select * from countries")
      )
      res.finalSql mustBe "select * from products join country on product.country_id = country.id"
    }
  }

}
