/*
 * Copyright 2014-2015 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Created on 2014年3月25日
// $Id$
package com.wplatform.ddal.config.parser;

import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ConfigEntityResolver implements EntityResolver {

    private static final Map<String, String> doctypeMap = new HashMap<String, String>();

    private static final String MAPPER_DOCTYPE = "-//wplatform.com//DTD ddal-config//EN"
            .toUpperCase(Locale.ENGLISH);

    private static final String SNF_DAL_MAPPER_URL = "http://wplatform.com/dtd/ddal-config.dtd"
            .toUpperCase(Locale.ENGLISH);

    private static final String SNF_DAL_MAPPER_DTD = "/META-INF/ddal-config.dtd";

    static {
        doctypeMap.put(SNF_DAL_MAPPER_URL, SNF_DAL_MAPPER_DTD);
        doctypeMap.put(MAPPER_DOCTYPE, SNF_DAL_MAPPER_DTD);
    }


    /**
     * Converts a public DTD into a local one
     *
     * @param publicId Unused but required by EntityResolver interface
     * @param systemId The DTD that is being requested
     * @return The InputSource for the DTD
     * @throws org.xml.sax.SAXException If anything goes wrong
     */
    public InputSource resolveEntity(String publicId, String systemId) throws SAXException {
        if (publicId != null) {
            publicId = publicId.toUpperCase(Locale.ENGLISH);
        }
        if (systemId != null) {
            systemId = systemId.toUpperCase(Locale.ENGLISH);
        }
        InputSource source = null;
        try {
            String path = doctypeMap.get(publicId);
            source = getInputSource(path);
            if (source == null) {
                path = doctypeMap.get(systemId);
                source = getInputSource(path);
            }
        } catch (Exception e) {
            throw new SAXException(e.toString());
        }
        return source;
    }

    private InputSource getInputSource(String path) {
        InputSource source = null;
        if (path != null) {
            InputStream in;
            try {
                in = this.getClass().getResourceAsStream(path);
                source = new InputSource(in);
            } catch (Exception e) {
                // ignore, it is ok that the InputSource is null. 
            }
        }
        return source;
    }

}