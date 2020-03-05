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

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Tests if the JDBC database persists a prepared XA transaction when the
 * client disconnects.
 * <p>
 * You need to add your XADataSource to the first line of the main() method.
 */
public final class JdbcXaTest {

    private JdbcXaTest() { }

    /** */
    public static void main(String[] args) throws Exception {
        // replace this line with a factory for your database, for example:
        //    PGXADataSource factory = new PGXADataSource();
        XADataSource factory = null;

        // create an xa-connection, connection and XA transaction
        XAConnection xaConn = factory.getXAConnection();
        Connection conn = xaConn.getConnection();
        XAResource xaRes = xaConn.getXAResource();
        Xid xid = new MyXid(1);

        // create a table "foo"
        Statement stmt = conn.createStatement();
        stmt.execute("create table foo(a numeric)");

        // start the transaction and insert one record
        xaRes.start(xid, XAResource.TMNOFLAGS);
        stmt.execute("insert into foo values (1)");
        xaRes.end(xid, XAResource.TMSUCCESS);
        xaRes.prepare(xid);

        // Now disconnect. Some brokers roll back the transaction, but this is not
        // compatible with Jet's fault tolerance.
        xaConn.close();

        // connect again
        xaConn = factory.getXAConnection();
        conn = xaConn.getConnection();
        xaRes = xaConn.getXAResource();

        // commit the prepared transaction
        xaRes.commit(xid, false);

        // check that record is inserted
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select a from foo");
        if (!rs.next() || rs.getInt(1) != 1) {
            System.err.println("Failure: record missing or has wrong value");
        } else {
            System.out.println("Success!");
        }
    }
}
