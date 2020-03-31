/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.contrib.xatests;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
//import org.apache.activemq.ActiveMQXAConnectionFactory;

/**
 * Tests if the JMS broker persists a prepared XA transaction when the client
 * disconnects.
 * <p>
 * You need to add your XADataSource to getXAConnectionFactory() method.
 */
public final class JmsXaTest {

    private static final long TIMEOUT = 5000;

    private JmsXaTest() { }

    /**
     * Configure factory for broker here.
     */
    private static XAConnectionFactory getXAConnectionFactory() {
        // replace this line with a factory for your broker, for example:
        //    ActiveMQXAConnectionFactory factory = new ActiveMQXAConnectionFactory(BROKER_URL);
        //    return factory;
        return null;
    }

    /** */
    public static void main(String[] args) throws Exception {
        XAConnectionFactory factory = getXAConnectionFactory();

        if (factory == null) {
            throw new IllegalArgumentException("Provide factory for broker in getXAConnectionFactory() method");
        }

        // create a connection, session and XA transaction
        XAConnection conn = factory.createXAConnection();
        XASession sess = conn.createXASession();
        XAResource xaRes = sess.getXAResource();
        Xid xid = new MyXid(1);

        // start the transaction and produce one message
        xaRes.start(xid, XAResource.TMNOFLAGS);
        MessageProducer producer = sess.createProducer(sess.createQueue("queue"));
        producer.send(sess.createTextMessage("foo"));
        xaRes.end(xid, XAResource.TMSUCCESS);

        // prepare the transaction
        xaRes.prepare(xid);

        // now disconnect. Some brokers roll back the transaction, but this is not
        // compatible with Jet's fault tolerance.
        conn.close();

        // connect again
        conn = factory.createXAConnection();
        conn.start();
        sess = conn.createXASession();
        xaRes = sess.getXAResource();

        // commit the prepared transaction
        xaRes.commit(xid, false);

        // check that the message is there
        MessageConsumer cons = sess.createConsumer(sess.createQueue("queue"));
        TextMessage msg = (TextMessage) cons.receive(TIMEOUT);
        if (msg == null || !msg.getText().equals("foo")) {
            System.err.println("Message is missing or has wrong text, transaction probably lost");
        } else {
            System.out.println("Success!");
        }
        conn.close();
    }
}
