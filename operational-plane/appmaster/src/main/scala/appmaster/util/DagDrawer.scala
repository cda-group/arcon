package appmaster.util

import com.github.mdr.ascii.graph.Graph
import com.github.mdr.ascii.layout.GraphLayout
import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
import se.kth.cda.compiler.dataflow.DFG
import se.kth.cda.compiler.dataflow.NodeKind._
import se.kth.cda.compiler.dataflow.Node

private[appmaster] object DagDrawer {

  def logicalDAG(dfg: DFG): String = {
    val vertices = scala.collection.mutable.SortedSet[String]()
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[(String, String)]()

    // TODO: actually implement this when JSON is complete
    /*
    dfg.nodes.foreach { node => 
      node match {
        case Node(id, _, Source(sType, strategy, successors, kind)) => 
          vertices += "source"
          edges += "source" → "yeha"
          ()
        case Node(id, _, Task(_,_,_,_,_,_,_,_)) => 
          vertices += "task"
          edges += "task" → "yeha"
        case Node(id, _, Window(_, _, _, _, _, _, _)) => 
          vertices += "window"
          edges += "window" → "yeha"
        case Node(id, _, Sink(sType, predecessor, kind)) => 
          vertices += "sink"
      }
    }
    */

    val prefs = LayoutPrefsImpl(vertical = false, elevateEdges = false)
    val graph = Graph(vertices = vertices.toSet, edges.toList)
    GraphLayout.renderGraph(graph, layoutPrefs = prefs)
  }
}
