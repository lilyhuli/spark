package ignite

import org.apache.ignite.Ignition

/**
 * @title: ExampleNodeStarup
 * @projectName spark
 * @description: TODO
 * @author tangd-a
 * @date 2019/11/2820:15
 */
object ExampleNodeStartup {
  def main(args: Array[String]): Unit = {
    import org.apache.ignite.Ignition
    Ignition.start("config/example-ignite.xml")
  }
}
