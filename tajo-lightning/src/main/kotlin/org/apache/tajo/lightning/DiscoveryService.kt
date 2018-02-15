package org.apache.tajo.lightning

import org.apache.tajo.lightning.conf.Config

class DiscoveryService: Service {

    init {
        val nodeManager = NodeManager()
    }

    override fun init(conf: Config) {
    }

    override fun start() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun stop() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}