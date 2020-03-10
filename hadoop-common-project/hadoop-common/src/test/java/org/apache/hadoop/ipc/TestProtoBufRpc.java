/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ipc;

import org.apache.hadoop.thirdparty.protobuf.BlockingService;
import org.apache.hadoop.thirdparty.protobuf.RpcController;
import org.apache.hadoop.thirdparty.protobuf.ServiceException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ipc.metrics.RpcMetrics;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.TestProtos;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto;
import org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpc2Proto;
import org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.apache.hadoop.test.MetricsAsserts.assertCounterGt;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import static org.junit.Assert.fail;

/**
 * Test for testing protocol buffer based RPC mechanism.
 * This test depends on test.proto definition of types in src/test/proto
 * and protobuf service definition from src/test/test_rpc_service.proto
 */
public class TestProtoBufRpc extends TestRpcBase {
  private static RPC.Server server;
  private final static int SLEEP_DURATION = 1000;

  @ProtocolInfo(protocolName = "testProto2", protocolVersion = 1)
  public interface TestRpcService2 extends
      TestProtobufRpc2Proto.BlockingInterface {
  }

  public static class PBServer2Impl implements TestRpcService2 {

    @Override
    public EmptyResponseProto ping2(RpcController unused,
        EmptyRequestProto request) throws ServiceException {
      return EmptyResponseProto.newBuilder().build();
    }

    @Override
    public EchoResponseProto echo2(RpcController unused, EchoRequestProto request)
        throws ServiceException {
      return EchoResponseProto.newBuilder().setMessage(request.getMessage())
          .build();
    }

    @Override
    public TestProtos.SleepResponseProto sleep(RpcController controller,
      TestProtos.SleepRequestProto request) throws ServiceException {
      try{
        Thread.sleep(request.getMilliSeconds());
      } catch (InterruptedException ex){
      }
      return  TestProtos.SleepResponseProto.newBuilder().build();
    }
  }

  @Before
  public void setUp() throws IOException { // Setup server for both protocols
    conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, 1024);
    conf.setBoolean(CommonConfigurationKeys.IPC_SERVER_LOG_SLOW_RPC, true);
    // Set RPC engine to protobuf RPC engine
    RPC.setProtocolEngine(conf, TestRpcService.class, ProtobufRpcEngine.class);
    RPC.setProtocolEngine(conf, TestRpcService2.class, ProtobufRpcEngine.class);

    // Create server side implementation
    PBServerImpl serverImpl = new PBServerImpl();
    BlockingService service = TestProtobufRpcProto
        .newReflectiveBlockingService(serverImpl);

    // Get RPC server for server side implementation
    server = new RPC.Builder(conf).setProtocol(TestRpcService.class)
        .setInstance(service).setBindAddress(ADDRESS).setPort(PORT).build();
    addr = NetUtils.getConnectAddress(server);

    // now the second protocol
    PBServer2Impl server2Impl = new PBServer2Impl();
    BlockingService service2 = TestProtobufRpc2Proto
        .newReflectiveBlockingService(server2Impl);

    server.addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER, TestRpcService2.class,
        service2);
    server.start();
  }
  
  
  @After
  public void tearDown() throws Exception {
    server.stop();
  }

  private TestRpcService2 getClient2() throws IOException {
    return RPC.getProxy(TestRpcService2.class, 0, addr, conf);
  }

  @Test (timeout=5000)
  public void testProtoBufRpc() throws Exception {
    TestRpcService client = getClient(addr, conf);
    testProtoBufRpc(client);
  }
  
  // separated test out so that other tests can call it.
  public static void testProtoBufRpc(TestRpcService client) throws Exception {  
    // Test ping method
    client.ping(null, newEmptyRequest());
    
    // Test echo method
    EchoRequestProto echoRequest = EchoRequestProto.newBuilder()
        .setMessage("hello").build();
    EchoResponseProto echoResponse = client.echo(null, echoRequest);
    assertThat(echoResponse.getMessage()).isEqualTo("hello");
    
    // Test error method - error should be thrown as RemoteException
    try {
      client.error(null, newEmptyRequest());
      fail("Expected exception is not thrown");
    } catch (ServiceException e) {
      RemoteException re = (RemoteException)e.getCause();
      RpcServerException rse = (RpcServerException) re
          .unwrapRemoteException(RpcServerException.class);
      assertThat(rse).isNotNull();
      assertThat(re.getErrorCode())
          .isEqualTo(RpcErrorCodeProto.ERROR_RPC_SERVER);
    }
  }
  
  @Test (timeout=5000)
  public void testProtoBufRpc2() throws Exception {
    TestRpcService2 client = getClient2();
    
    // Test ping method
    client.ping2(null, newEmptyRequest());
    
    // Test echo method
    EchoResponseProto echoResponse = client.echo2(null,
        newEchoRequest("hello"));
    assertThat(echoResponse.getMessage()).isEqualTo("hello");
    
    // Ensure RPC metrics are updated
    MetricsRecordBuilder rpcMetrics = getMetrics(server.getRpcMetrics().name());
    assertCounterGt("RpcQueueTimeNumOps", 0L, rpcMetrics);
    assertCounterGt("RpcProcessingTimeNumOps", 0L, rpcMetrics);
    
    MetricsRecordBuilder rpcDetailedMetrics = 
        getMetrics(server.getRpcDetailedMetrics().name());
    assertCounterGt("Echo2NumOps", 0L, rpcDetailedMetrics);
  }

  @Test (timeout=5000)
  public void testProtoBufRandomException() throws Exception {
    TestRpcService client = getClient(addr, conf);

    try {
      client.error2(null, newEmptyRequest());
    } catch (ServiceException se) {
      assertThat(se.getCause()).isInstanceOf(RemoteException.class);
      RemoteException re = (RemoteException) se.getCause();
      assertThat(re.getClassName())
          .isEqualTo(URISyntaxException.class.getName());
      assertThat(re.getMessage()).contains("testException");
      assertThat(re.getErrorCode())
          .isEqualTo(RpcErrorCodeProto.ERROR_APPLICATION);
    }
  }
  
  @Test(timeout=6000)
  public void testExtraLongRpc() throws Exception {
    TestRpcService2 client = getClient2();
    final String shortString = StringUtils.repeat("X", 4);
    // short message goes through
    EchoResponseProto echoResponse = client.echo2(null,
        newEchoRequest(shortString));
    assertThat(echoResponse.getMessage()).isEqualTo(shortString);
    
    final String longString = StringUtils.repeat("X", 4096);
    try {
      client.echo2(null, newEchoRequest(longString));
      fail("expected extra-long RPC to fail");
    } catch (ServiceException se) {
      // expected
    }
  }

  @Test(timeout = 12000)
  public void testLogSlowRPC() throws IOException, ServiceException,
      TimeoutException, InterruptedException {
    TestRpcService2 client = getClient2();
    // make 10 K fast calls
    for (int x = 0; x < 10000; x++) {
      try {
        client.ping2(null, newEmptyRequest());
      } catch (Exception ex) {
        throw ex;
      }
    }

    // Ensure RPC metrics are updated
    RpcMetrics rpcMetrics = server.getRpcMetrics();
    assertThat(rpcMetrics.getProcessingSampleCount()).isGreaterThan(999L);
    long before = rpcMetrics.getRpcSlowCalls();

    // make a really slow call. Sleep sleeps for 1000ms
    client.sleep(null, newSleepRequest(SLEEP_DURATION * 3));

    // Ensure slow call is logged.
    GenericTestUtils.waitFor(()
        -> rpcMetrics.getRpcSlowCalls() == before + 1L, 10, 1000);
  }

  @Test(timeout = 12000)
  public void testEnsureNoLogIfDisabled() throws IOException, ServiceException {
    // disable slow RPC  logging
    server.setLogSlowRPC(false);
    TestRpcService2 client = getClient2();

    // make 10 K fast calls
    for (int x = 0; x < 10000; x++) {
      client.ping2(null, newEmptyRequest());
    }

    // Ensure RPC metrics are updated
    RpcMetrics rpcMetrics = server.getRpcMetrics();
    assertThat(rpcMetrics.getProcessingSampleCount()).isGreaterThan(999L);
    long before = rpcMetrics.getRpcSlowCalls();

    // make a really slow call. Sleep sleeps for 1000ms
    client.sleep(null, newSleepRequest(SLEEP_DURATION));

    long after = rpcMetrics.getRpcSlowCalls();

    // make sure we never called into Log slow RPC routine.
    assertThat(before).isEqualTo(after);
  }
}
