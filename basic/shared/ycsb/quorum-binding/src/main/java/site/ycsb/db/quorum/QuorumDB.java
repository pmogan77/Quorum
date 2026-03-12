package site.ycsb.db.quorum;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import kv.AgentKVGrpc;
import kv.Kv;

import site.ycsb.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class QuorumDB extends DB {

    private ManagedChannel channel;
    private AgentKVGrpc.AgentKVBlockingStub stub;

    private String coordinatorHost;
    private int coordinatorPort;

    @Override
    public void init() throws DBException {

        Properties props = getProperties();

        coordinatorHost = props.getProperty("coord.host", "10.0.0.101");
        coordinatorPort = Integer.parseInt(
                props.getProperty("coord.port", "6000")
        );

        channel = ManagedChannelBuilder
                .forAddress(coordinatorHost, coordinatorPort)
                .usePlaintext()
                .build();

        stub = AgentKVGrpc.newBlockingStub(channel);
    }

    @Override
    public void cleanup() throws DBException {

        if (channel != null) {

            try {

                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {

                throw new DBException(e);
            }
        }
    }

    @Override
    public Status read(String table,
                       String key,
                       Set<String> fields,
                       Map<String, ByteIterator> result) {

        Kv.AgentGetRequest req =
                Kv.AgentGetRequest.newBuilder()
                        .setKey(key)
                        .build();

        Kv.AgentGetReply reply;

        try {

            reply = stub.get(req);

        } catch (Exception e) {

            return Status.ERROR;
        }

        switch (reply.getStatus()) {

            case OK:

                result.put("field0",
                        new StringByteIterator(reply.getValue()));

                return Status.OK;

            case NOT_FOUND:

                return Status.NOT_FOUND;

            case QUORUM_FAILED:

                return Status.SERVICE_UNAVAILABLE;

            default:

                return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table,
                         String key,
                         Map<String, ByteIterator> values) {

        ByteIterator val = values.get("field0");

        if (val == null) {
            return Status.ERROR;
        }

        Kv.AgentPutRequest req =
                Kv.AgentPutRequest.newBuilder()
                        .setKey(key)
                        .setValue(val.toString())
                        .build();

        Kv.AgentPutReply reply;

        try {

            reply = stub.put(req);

        } catch (Exception e) {

            return Status.ERROR;
        }

        return reply.getSuccess()
                ? Status.OK
                : Status.ERROR;
    }

    @Override
    public Status update(String table,
                         String key,
                         Map<String, ByteIterator> values) {

        return insert(table, key, values);
    }

    @Override
    public Status delete(String table, String key) {

        Kv.AgentDeleteRequest req =
                Kv.AgentDeleteRequest.newBuilder()
                        .setKey(key)
                        .build();

        Kv.AgentDeleteReply reply;

        try {

            reply = stub.delete(req);

        } catch (Exception e) {

            return Status.ERROR;
        }

        return reply.getSuccess()
                ? Status.OK
                : Status.ERROR;
    }

    @Override
    public Status scan(String table,
                       String startkey,
                       int recordcount,
                       Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {

        return Status.NOT_IMPLEMENTED;
    }
}