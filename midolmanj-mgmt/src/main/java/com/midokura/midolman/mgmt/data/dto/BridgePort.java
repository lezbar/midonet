/*
 * Copyright 2012 Midokura KK
 * Copyright 2012 Midokura PTE LTD.
 */
package com.midokura.midolman.mgmt.data.dto;

import java.net.URI;
import java.util.UUID;

import com.midokura.midolman.mgmt.data.dto.config.PortMgmtConfig;
import com.midokura.midolman.mgmt.rest_api.core.ResourceUriBuilder;
import com.midokura.midolman.state.PortDirectory.BridgePortConfig;

/**
 * Class representing a bridge port.
 */
public abstract class BridgePort extends Port {

    /**
     * Default constructor
     */
    public BridgePort() {
        super();
    }

    /**
     * Constructor
     *
     * @param id
     *            ID of port
     */
    public BridgePort(UUID id, UUID deviceId) {
        super(id, deviceId);
    }

    /**
     * Constructor
     *
     * @param id
     * @param config
     * @param mgmtConfig
     */
    public BridgePort(UUID id, BridgePortConfig config,
            PortMgmtConfig mgmtConfig) {
        super(id, config, mgmtConfig);
    }

    /**
     * @param config
     *            BridgePortConfig object to set.
     */
    public void setConfig(BridgePortConfig config) {
        super.setConfig(config);
    }

    /**
     * @return the bridge URI
     */
    @Override
    public URI getDevice() {
        if (getBaseUri() != null && deviceId != null) {
            return ResourceUriBuilder.getBridge(getBaseUri(), deviceId);
        } else {
            return null;
        }
    }

    @Override
    public boolean isRouterPort() {
        return false;
    }
}
