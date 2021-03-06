/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.monitor;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.ServiceLoader;

public class MonitorModule extends AbstractModule {

    private final Settings settings;
    private final Logger logger;

    public MonitorModule(Settings settings) {
        this.logger = Loggers.getLogger(MonitorModule.class, settings);
        this.settings = settings;
    }

    @Override
    protected void configure() {
        boolean bound = false;
        for (NodeInfoLoader nodeInfoLoader : ServiceLoader.load(NodeInfoLoader.class)) {
            Class<? extends ExtendedNodeInfo> extendedNodeInfoClass = nodeInfoLoader.getExtendedNodeInfoClass(settings);
            if (extendedNodeInfoClass == null) {
                continue;
            }
            if (bound) {
                throw new IllegalArgumentException("Multiple ExtendedNodeInfo implementations found");
            }
            bind(ExtendedNodeInfo.class).to(extendedNodeInfoClass);
            bound = true;
        }
        if (!bound) {
            logger.info("Sigar not available. CPU/DISK/Network stats won't be available");
            bind(ExtendedNodeInfo.class).to(ZeroExtendedNodeInfo.class).asEagerSingleton();
        }
    }
}
