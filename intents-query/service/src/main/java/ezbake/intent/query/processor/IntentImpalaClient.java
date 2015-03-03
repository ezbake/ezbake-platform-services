/*   Copyright (C) 2013-2015 Computer Sciences Corporation
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
 * limitations under the License. */

package ezbake.intent.query.processor;

import com.cloudera.impala.thrift.ImpalaHiveServer2Service;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class IntentImpalaClient {
    private static String host = "localhost";
    private static int port = 21050;
    private Logger appLog = LoggerFactory.getLogger(IntentImpalaClient.class);

    public IntentImpalaClient() {
    }

    public void shutdown() {

    }

    public List<TRow> queryImpala(String qryString) throws TException {

        List<TRow> resultRows;

        TSocket transport = new TSocket(host, port);

        transport.setTimeout(60000);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        ImpalaHiveServer2Service.Client client = new ImpalaHiveServer2Service.Client.Factory().getClient(protocol);

        try {
            transport.open();
        } catch (TTransportException e) {
            appLog.error("open transport exception: ", e);
            transport.close();
            throw new TException(e);
        }

        TOpenSessionReq openReq = new TOpenSessionReq();
        openReq.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
        //openReq.setUsername(username);
        //openReq.setPassword(password);

        TOpenSessionResp openResp;
        org.apache.hive.service.cli.thrift.TStatus status;

        try {
            openResp = client.OpenSession(openReq);
            status = openResp.getStatus();
            if (status.getStatusCode() == org.apache.hive.service.cli.thrift.TStatusCode.ERROR_STATUS) {
                throw new TException("failed open session: " + status.toString());
            }
        } catch (TException e) {
            appLog.error("open session error: ", e);
            closeConnection(transport, client, null, null);
            throw e;
        }


        TSessionHandle sessHandle = openResp.getSessionHandle();
        TOperationHandle stmtHandle = null;

        TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, qryString);
        try {
            TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
            status = execResp.getStatus();
            if (status.getStatusCode() == org.apache.hive.service.cli.thrift.TStatusCode.ERROR_STATUS) {
                throw new TException("failed execute statement: " + status.toString());
            }

            stmtHandle = execResp.getOperationHandle();

            if (stmtHandle == null) {
                throw new TException("failed get operation handle");
            }
        } catch (TException e) {
            appLog.error("ExecuteStatement exception: ", e);
            closeConnection(transport, client, stmtHandle, sessHandle);
            throw e;
        }

        TFetchResultsReq fetchReq = new TFetchResultsReq();
        fetchReq.setOperationHandle(stmtHandle);
//        fetchReq.setMaxRows(100);
//        fetchReq.setOrientation(org.apache.hive.service.cli.thrift.TFetchOrientation.FETCH_NEXT);

        try {
            TFetchResultsResp resultsResp = client.FetchResults(fetchReq);

            status = resultsResp.getStatus();
            if (status.getStatusCode() == org.apache.hive.service.cli.thrift.TStatusCode.ERROR_STATUS) {
                throw new TException("failed fetch results: " + status.toString());
            }

            TRowSet resultsSet = resultsResp.getResults();
            resultRows = resultsSet.getRows();
            appLog.info(String.format("Total rows returned = %d", resultRows.size()));

            closeConnection(transport, client, stmtHandle, sessHandle);
        } catch (TException e) {
            appLog.error("FetchResults exception: ", e);
            closeConnection(transport, client, stmtHandle, sessHandle);
            throw e;
        }

        return resultRows;
    }

    public void closeConnection(TSocket transport, ImpalaHiveServer2Service.Client client, TOperationHandle stmtHandle, TSessionHandle sessHandle) {
        if (client != null && stmtHandle != null) {
            try {
                TCloseOperationReq closeReq = new TCloseOperationReq(stmtHandle);
                client.CloseOperation(closeReq);
            } catch (TException e) {
                appLog.error("failed close operation", e);
            }
        }

        if (client != null && sessHandle != null) {
            try {
                TCloseSessionReq closeConnectionReq = new TCloseSessionReq(sessHandle);
                client.CloseSession(closeConnectionReq);
            } catch (TException e) {
                appLog.error("failed close session", e);
            }
        }
        if (transport != null) {
            transport.close();
        }
    }
}
