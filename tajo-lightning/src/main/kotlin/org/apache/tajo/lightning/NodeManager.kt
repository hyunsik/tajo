package org.apache.tajo.lightning

import org.apache.tajo.lightning.conf.Node
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap

data class NodeId(val uuid: String)

enum class NodeState { ACTIVE, INACTIVE }

class Node(id: NodeId, state: NodeState, host: String, port: Int) {
    fun update() {}

    fun schedule() {}

    fun kill() {}
}

class NodeManager {
    val nodes: ConcurrentHashMap<NodeId, Node> = ConcurrentHashMap<NodeId, Node>()

    fun add(node: Node) {}

    fun remove(node: Node) {}
}