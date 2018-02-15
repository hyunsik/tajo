package org.apache.tajo.lightning.conf

import com.moandjiezana.toml.Toml
import java.io.File

data class Config(val coordinator: Coordinator, val node: Node) {
    companion object Factory {
        fun load(conf: String): Config {
            return Toml().read(conf).to(Config::class.java)
        }

        fun load(conf: File): Config {
            return Toml().read(conf).to(Config::class.java)
        }
    }
}

data class Coordinator(val enabled: Boolean, val uri: String)

data class Node(val http: Http, val executor: Executor)

data class Http(val listen: String)

data class Executor(val data_dir: String)

