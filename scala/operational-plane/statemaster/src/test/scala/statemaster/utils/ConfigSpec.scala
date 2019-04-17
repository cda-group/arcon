package statemaster.utils

import org.scalatest.FlatSpec

class ConfigSpec extends FlatSpec with Config {

  "Statemaster Config" should "be functional" in {
    assert(config.isResolved)
  }
}
