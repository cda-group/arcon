package appmaster.util

import java.awt.Font

final case class Settings(font: Font, width: Int, height: Int)

sealed trait AsciiFont
case object Jokerman extends AsciiFont
case object Garamond extends AsciiFont

case object AsciiSettings {
  final val jokerman = "Jokerman"
  final val garamond = "Garamond"
  def apply(width: Int = 50, height: Int = 50, fontSize: Int = 12, font: AsciiFont = Jokerman): Settings = {
    font match {
      case Jokerman =>
        val font = new Font(jokerman, Font.PLAIN, fontSize)
        Settings(font, width, height)
      case Garamond =>
        val font = new Font(garamond, Font.PLAIN, fontSize)
        Settings(font, width, height)
    }
  }
}
