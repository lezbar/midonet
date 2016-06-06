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

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedDeque, ExecutorService, ScheduledExecutorService, TimeUnit}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import com.google.inject.Inject
import com.google.inject.name.Named

import rx.schedulers.Schedulers
import rx.{Observable, Observer, Subscription}
import rx.subjects.PublishSubject

import org.midonet.cluster.models.Neutron.{NeutronPort, NeutronSubnet}
import org.midonet.cluster.models.State.ContainerStatus.Code
import org.midonet.cluster.services.MidonetBackend
import org.midonet.cluster.util.IPAddressUtil._
import org.midonet.containers.RallyContainerHandler.{Config, RallyContainerException}
import org.midonet.packets.IPv4Addr
import org.midonet.util.functors.makeRunnable

object RallyContainerHandler {


    final val SshHelperScript = "/usr/lib/midolman/plugins/rally-helper"

    /**
      * Stores the configuration for the SSH container and provides access to
      * the container script commands.
      */
    case class Config(name: String,
                      mac: String,
                      address: Option[String],
                      gateway: Option[String],
                      target: Option[String]) {
        val createCommand = {
            val command = new StringBuilder(s"$SshHelperScript create " +
                                            s"-n $name -m $mac ")
            if (address.isDefined) command append s"-a ${address.get} "
            if (gateway.isDefined) command append s"-g ${gateway.get} "
            command.toString()
        }

        val deleteCommand = s"$SshHelperScript delete -n $name"

        val statusCommand = {
            val command = new StringBuilder(s"$SshHelperScript status -n $name ")
            if (target.isDefined) command append s"-t ${target.get}"
            command.toString()
        }
    }

    case class RallyContainerException(message: String) extends Exception(message)

}

@Container(name = "RALLY", version = 1)
class RallyContainerHandler @Inject()(@Named("id") id: UUID,
                                      @Named("container") containerExecutor: ExecutorService,
                                      @Named("io") ioExecutor: ScheduledExecutorService,
                                      backend: MidonetBackend)
    extends ContainerHandler with ContainerCommons {

    override def logSource = "org.midonet.containers.rally"

    private implicit val ec = ExecutionContext.fromExecutor(containerExecutor)
    private val scheduler = Schedulers.from(containerExecutor)
    @volatile private var config: Config = null

    private val statusSubject = PublishSubject.create[ContainerStatus]
    @volatile private var statusSubscription: Subscription = null
    private val statusQueue = new ConcurrentLinkedDeque[() => Unit]()
    private val statusObserver = new Observer[java.lang.Long] {
        override def onNext(tick: java.lang.Long): Unit = {
            if (config eq null) {
                return
            }
            if (config.target.isEmpty) {
                return
            }
            statusQueue.offer(() => {
                try {
                    val status = checkStatus()
                    log debug s"Rally container status: $status"
                    statusSubject onNext status
                } catch {
                    case NonFatal(e) =>
                        log.info("Rally container status check failed", e)
                }
            })
            ioExecutor.execute(makeRunnable {
                val function = statusQueue.peek()
                if (function ne null) {
                    function()
                }
            })
        }

        override def onCompleted(): Unit = {
            log warn "Container status scheduling completed unexpectedly"
        }

        override def onError(e: Throwable): Unit = {
            log.error("Container status scheduling error", e)
        }
    }

    private def store = backend.store

    override def create(port: ContainerPort): Future[Option[String]] = {

        val configMsb = port.configurationId.getMostSignificantBits

        val hasDhcp = (configMsb & 0x100000000L) != 0
        val hasTarget = (configMsb & 0xFFFFFFFF) != 0
        val statusInterval = ((configMsb & 0xFF0000000000L) >> 40) * 10000
        val targetAddress = IPv4Addr((configMsb & 0xFFFFFFFFL).toInt).toString

        log info s"Creating Rally container for port ${port.portId} " +
                 s"interface ${port.interfaceName} " +
                 s"configuration ${port.configurationId} " +
                 s"status every $statusInterval ms"

        val subscription = statusSubscription
        if (subscription ne null) {
            subscription.unsubscribe()
            statusSubscription = null
        }

        store.get(classOf[NeutronPort], port.portId) flatMap { neutronPort =>

            if (neutronPort.getFixedIpsCount != 1) {
                throw RallyContainerException(
                    s"Port ${port.portId} must have only one IP address")
            }

            val portMac = neutronPort.getMacAddress
            val portAddress = neutronPort.getFixedIps(0).getIpAddress.asString
            val subnetId = neutronPort.getFixedIps(0).getSubnetId

            store.get(classOf[NeutronSubnet], subnetId) map { neutronSubnet =>

                val subnetAddress = neutronSubnet.getCidr
                val portSubnetAddress =
                    s"$portAddress/${subnetAddress.getPrefixLength}"
                val gatewayAddress = neutronSubnet.getGatewayIp.asString

                config = Config(port.interfaceName,
                                portMac,
                                if (hasDhcp) None else Some(portSubnetAddress),
                                if (hasDhcp) None else Some(gatewayAddress),
                                if (hasTarget) Some(targetAddress) else None)

                // Clean the previous container if any and then create the
                // new container with a rollback of deleting the container.
                try {
                    statusSubject onNext ContainerOp(ContainerFlag.Created,
                                                     config.name)
                    executeCommands(Seq((config.deleteCommand, null),
                                        (config.createCommand,
                                            config.deleteCommand)))
                    statusSubject onNext ContainerHealth(Code.RUNNING,
                                                         config.name, "")

                    if (statusInterval > 0 && hasTarget) {
                        statusSubscription =
                            Observable.interval(statusInterval, statusInterval,
                                                TimeUnit.MILLISECONDS, scheduler)
                                      .subscribe(statusObserver)
                    }
                } catch {
                    case NonFatal(e) =>
                        statusSubject onNext ContainerOp(ContainerFlag.Deleted,
                                                         config.name)
                        throw e
                }
                Some(port.interfaceName)
            }
        }
    }

    override def updated(port: ContainerPort): Future[Option[String]] = {
        delete() flatMap { _ => create(port) }
    }

    override def delete(): Future[Unit] = {
        if (config eq null) {
            return Future.successful(None)
        }

        log info s"Deleting Rally container ${config.name}"

        statusQueue.clear()
        val subscription = statusSubscription
        if (subscription ne null) {
            subscription.unsubscribe()
            statusSubscription = null
        }
        statusSubject onNext ContainerOp(ContainerFlag.Deleted, config.name)

        try {
            execute(config.deleteCommand)
            Future.successful(Some(config.name))
        } catch {
            case NonFatal(e) =>
                log.warn(s"Failed to delete container ${config.name}", e)
                Future.failed(e)
        } finally {
            config = null
        }
    }

    override def cleanup(name: String): Future[Unit] = {
        log info s"Cleaning Rally container $name"
        try {
            execute(Config(name, mac = null, None, None, None).deleteCommand)
            Future.successful({})
        } catch {
            case NonFatal(e) =>
                log.warn(s"Failed to cleanup container $name", e)
                Future.failed(e)
        }
    }

    override def status: Observable[ContainerStatus] = {
        statusSubject.asObservable()
    }

    /**
      * Synchronous method that checks the current status of the IPSec process,
      * and returns the status as a string.
      */
    private def checkStatus(): ContainerHealth = {
        if (null == config) {
            return ContainerHealth(Code.STOPPED, "", "")
        }
        val (code, out) = executeWithOutput(config.statusCommand)
        val currentConfig = config
        if (null == currentConfig) {
            return ContainerHealth(Code.STOPPED, "", "")
        }
        if (code == 0) {
            ContainerHealth(Code.RUNNING, currentConfig.name, out)
        } else {
            ContainerHealth(Code.RUNNING, currentConfig.name, out)
        }
    }

}
