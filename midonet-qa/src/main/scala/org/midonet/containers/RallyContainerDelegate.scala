/*
 * Copyright 2016 Midokura SARL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.midonet.containers

import java.util.{ConcurrentModificationException, UUID}

import javax.annotation.Nullable

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.inject.Inject

import org.midonet.cluster.data.storage.{NotFoundException, Transaction}
import org.midonet.cluster.models.State
import org.midonet.cluster.models.Topology.{Host, Port, ServiceContainer}
import org.midonet.cluster.services.MidonetBackend
import org.midonet.cluster.util.UUIDUtil._

@Container(name = "RALLY", version = 1)
class RallyContainerDelegate @Inject()(backend: MidonetBackend)
    extends ContainerDelegate with ContainerCommons {

    final val MaxStorageAttempts = 10

    override def logSource = "org.midonet.cluster.services.containers.rally"

    @throws[Exception]
    override def onScheduled(container: ServiceContainer, hostId: UUID): Unit = {
        val containerId = container.getId.asJava
        if (!container.hasPortId) {
            throw new IllegalArgumentException(
                s"Rally container $containerId is not connected to a port")
        }
        val portId = container.getPortId.asJava
        val interfaceName = s"ral-${portId.toString.substring(0, 8)}"

        log info s"Rally container ${container.getId.asJava} scheduled at host " +
                 s"$hostId: binding port $portId to interface $interfaceName"
        tryTx { tx =>
            val port = tx.get(classOf[Port], portId)
            val builder = port.toBuilder.setHostId(hostId.asProto)

            if (!port.hasInterfaceName) {
                // If the interface name is not set, set it for backwards
                // compatibility.
                builder.setInterfaceName(interfaceName)
            }

            // Check the host does not have another port bound to the same
            // interface.
            val host = tx.get(classOf[Host], hostId.asProto)
            val hostPorts = tx.getAll(classOf[Port], host.getPortIdsList.asScala)
            for (hostPort <- hostPorts
                 if hostPort.getInterfaceName == interfaceName) {
                log warn s"Host $hostId already has port ${hostPort.getId.asJava} " +
                         s"bound to interface $interfaceName"
            }
            tx update builder.build()
        }
    }

    override def onUp(container: ServiceContainer,
                      status: State.ContainerStatus): Unit = {
        log debug s"Rally container up at host ${status.getHostId.asJava} namespace " +
                  s"${status.getNamespaceName} interface ${status.getInterfaceName}"
    }

    override def onDown(container: ServiceContainer,
                        @Nullable status: State.ContainerStatus): Unit = {
        val hostId = if (status ne null) status.getHostId.asJava else null
        log debug s"Rally container down at host $hostId"
    }

    @throws[Exception]
    override def onUnscheduled(container: ServiceContainer, hostId: UUID): Unit = {
        val containerId = container.getId.asJava
        if (!container.hasPortId) {
            throw new IllegalArgumentException(
                s"Rally container $containerId is not connected to a port")
        }

        log info s"Rally container $containerId unscheduled from host $hostId: " +
                 "unbinding port"
        tryTx { tx =>
            val port = tx.get(classOf[Port], container.getPortId)
            if (port.hasHostId && port.getHostId.asJava == hostId) {
                tx update port.toBuilder.clearHostId().build()
            } else {
                log info s"Port ${container.getPortId.asJava} already " +
                         s"unbound from host $hostId"
            }
        } {
            case e: NotFoundException
                if e.clazz == classOf[Port] && e.id == container.getPortId =>
                log debug s"Port ${container.getPortId.asJava} already deleted"
        }
    }

    private def tryTx(f: (Transaction) => Unit)
                     (implicit handler: PartialFunction[Throwable, Unit] =
                     PartialFunction.empty): Unit = {
        var attempt = 1
        var last: Throwable = null
        while (attempt < MaxStorageAttempts) {
            try {
                val tx = backend.store.transaction()
                f(tx)
                tx.commit()
                return
            } catch {
                case e: ConcurrentModificationException =>
                    log warn s"Write $attempt of $MaxStorageAttempts failed due " +
                             s"to a concurrent modification (${e.getMessage})"
                    attempt += 1
                    last = e
                case NonFatal(e) if handler.isDefinedAt(e) =>
                    handler(e)
                    return
            }
        }
        throw last
    }

}
