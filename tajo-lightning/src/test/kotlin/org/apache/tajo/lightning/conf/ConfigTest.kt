package org.apache.tajo.lightning.conf

import org.junit.Test
import org.junit.Assert.*
import com.moandjiezana.toml.Toml
import org.apache.tajo.lightning.conf.*
import org.apache.tajo.lightning.conf.Node


class ConfigTest {
    @Test
    fun testLoad() {
        val conf = Config.load("""
[coordinator]
enabled = true
uri="http://localhost:8080"

[node]

[node.http]
listen="127.0.0.1:8000"

[node.executor]
data_dir="/tmp/flang/data"
            """);
        assertEquals(conf.coordinator, Coordinator(true, "http://localhost:8080"))
        assertEquals(conf.node, Node(Http("127.0.0.1:8000"), Executor("/tmp/flang/data")))
    }
}