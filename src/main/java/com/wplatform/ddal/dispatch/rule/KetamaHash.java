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
// Created on 2014骞�鏈�2鏃�
// $Id$

package com.wplatform.ddal.dispatch.rule;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class KetamaHash {

    private static final int NUM0 = 0;
    private static final int NUM1 = 1;
    private static final int NUM2 = 2;
    private static final int NUM3 = 3;
    private static final int NUM4 = 4;
    private static final int NUM8 = 8;
    private static final int NUM16 = 16;
    private static final int NUM24 = 24;
    private static final int NUM_0XFF = 0xFF;

    private static byte[] computeMd5(String k) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.reset();
            md5.update(k.getBytes("UTF-8"));
            return md5.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unknown string :" + k, e);
        }
    }

    public long calculate(Object paramObj) {
        String paramValue = paramObj == null ? "null" : paramObj.toString();
        long hashCode = HashAlgorithm.KETAMA_HASH.hash(computeMd5(paramValue), 0);
        return hashCode;
    }

    private enum HashAlgorithm {
        /**
         * MD5-based hash algorithm used by ketama.
         */
        KETAMA_HASH;

        public long hash(byte[] digest, int nTime) {
            long rv = ((long) (digest[NUM3 + nTime * NUM4] & NUM_0XFF) << NUM24)
                    | ((long) (digest[NUM2 + nTime * NUM4] & NUM_0XFF) << NUM16)
                    | ((long) (digest[NUM1 + nTime * NUM4] & NUM_0XFF) << NUM8)
                    | (digest[NUM0 + nTime * NUM4] & NUM_0XFF);
            return rv & 0xffffffffL; /* Truncate to 32-bits */
        }
    }

}
