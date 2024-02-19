package snorochevskiy.config

import com.typesafe.config.{Config, ConfigFactory}
import snorochevskiy.lang.OptionOps._
import snorochevskiy.lang.SyntaxOps.IdOps

import java.io.File
import java.nio.file.{Files, Paths}

case class AppConf(
  profile: String,
  useCatalog: CatalogConf,
  databases: Seq[DatabaseConf]
) {
  def findDb(alias: String): Option[DatabaseConf] =
    databases.find(_.alias.toLowerCase == alias.toLowerCase)
}

case class CatalogConf(catalogType: String, name: String)

case class DatabaseConf(
  alias: String,
  connectionUrl: String,
  user: String,
  password: String,
)

object ConfigProvider {

  /**
    * If spark-submit command has profiled specified like:
    * {{{
    * --conf "spark.driver.extraJavaOptions=-Dprofile=dev -DcustomResourceConf=stas -customFileConf=aaa.conf"
    * }}}
    * then config files will be loaded and applied one on top of another according to following scheme:
    * | file:     ./aaa.conf           |
    * | resource: stas-dev.conf        |
    * | resource: stas.conf            |
    * | resource: application-dev.conf |
    * | resource: application.conf     |
    *
    * (resources refers to src/main/resources/)
    * The profile is mandatory. Custom resource config, and custom file config are optional.
    */
  def loadMultilayeredConfig(profileName: String = null, customResource: String = null, customFile: String = null,
                             baseSrc: String = null): Config = {
    // TODO: add also an ability to load not only from resources but from files

    val base = Option(System.getProperty("baseSrc"))
      .map(p => if (p endsWith "/") p else s"$p/")
      .getOrElse("")

    val profile =
      if (System.getProperty("profile") ne null) System.getProperties.getProperty("profile")
      else if (profileName ne null) profileName
      else throw new IllegalStateException("Missing mandatory profile name")

    val profiledConf = Option(profile).map(p=>s"${base}application-${p}.conf")
    profiledConf.foreach { res =>
      if (getClass.getClassLoader.getResource(res) eq null)
        throw new IllegalStateException(s"Missing profiled config $res")
    }

    val customResName = Option(System.getProperty("customResourceConf")).orElse(customResource.opt)
    val customResConf = customResName.map(name=>s"${base}${name}.conf")
    val customResProfiledConf = customResName.map(name=>s"${base}${name}-${profile}.conf")
    (customResConf, customResProfiledConf).app { (res1, res2) => // If custom resource is specified, it should exist
      if (getClass.getClassLoader.getResource(res1) == null && getClass.getClassLoader.getResource(res2) == null)
        throw new IllegalStateException(s"Missing custom resource file $res1 or $res2")
    }

    val customFileConf = Option(System.getProperty("customFileConf")).orElse(customFile.opt)
    customFileConf.foreach { f => // If custom file is specified, it should exist
      if (Files.notExists(Paths.get(f))) throw new IllegalStateException(s"Missing config file: $f")
    }

    val configsHierarchy = ConfigFactory.load(s"${base}application.conf") ::
      List(profiledConf, customResConf, customResProfiledConf).flatten.map(ConfigFactory.parseResources) :::
      List(customFileConf.map(new File(_))).flatten.map(ConfigFactory.parseFile)

    configsHierarchy.reverse
      .reduce((acc, e) => acc.withFallback(e))
      .withFallback(ConfigFactory.parseString(s"app.profile=$profile"))
      .resolve()
  }


  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader._

  def loadAppConf(profileName: String = null, customResource: String = null, customFile: String = null,
                  baseSrc: String = "."): AppConf =
    loadMultilayeredConfig(profileName = profileName, customResource = customResource,
      customFile = customFile, baseSrc = baseSrc).as[AppConf]("app")

}
