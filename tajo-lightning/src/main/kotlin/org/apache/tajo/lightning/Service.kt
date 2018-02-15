package org.apache.tajo.lightning

import org.apache.tajo.exception.TajoError
import org.apache.tajo.exception.TajoException
import org.apache.tajo.lightning.conf.Config
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos
import java.io.IOException

interface Service {
    fun init(conf: Config)
    fun start()
    fun stop()
}

class ServiceException(val state: PrimitiveProtos.ReturnState): TajoException(state)