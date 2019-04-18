package common

import java.nio.ByteBuffer
import java.util.{Base64, UUID}

import scala.util.Random

object IdGenerator {

  def app(): String = randomName() + "_" + base64UUID()
  def container(): String = base64UUID()

  private def base64UUID(): String = {
    val id = UUID.randomUUID()
    val encoder = Base64.getUrlEncoder.withoutPadding()
    val src = ByteBuffer
      .wrap(new Array[Byte](16))
      .putLong(id.getMostSignificantBits)
      .putLong(id.getLeastSignificantBits)
      .array()

    encoder.encodeToString(src).substring(0, 22)
  }

  private final val names = Seq(
    "terminator",
    "neo",
    "han_solo",
    "joker",
    "batman",
    "jack_sparrow",
    "james_bond",
    "yoda",
    "gandalf",
    "forrest_gump",
    "harry_potter",
    "luke_skywalker",
    "legolas",
    "marty_mcfly",
    "the_dude",
    "shrek",
    "gollum",
    "mr_bean",
    "darth_vader",
    "ace_ventura",
    "walter_sobchak",
    "tony_montana",
    "woody",
    "bane",
    "jules_winfield",
    "john_wick",
    "robocop",
    "john_macclane",
    "totoro",
    "mclovin",
    "the_bride",
    "borat",
    "dr_evil",
    "ron_burgundy"
  )

  private def randomName(): String = {
    names(Random.nextInt(names.length))
  }

}
