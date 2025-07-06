package org.example.projectreactor.hello;

import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.tcp.TcpClient;

public class TcpClientTest {

    @Test
    public void test0() {
        // By default, the host is localhost and the port is 12012
        Connection connection = TcpClient.create() // Creates a TcpClient instance that is ready for configuring.
                .connectNow(); // 	Connects the client in a blocking fashion and waits for it to finish initializing.

        // The returned Connection offers a simple connection API, including disposeNow(), which shuts the client down in a blocking fashion.
        connection.onDispose()
                .block();
    }

    @Test
    public void test1() {
        Connection connection =
                TcpClient.create()
                        .host("example.com")
                        .port(80)
                        .connectNow();

        connection.onDispose()
                .block();
    }

    @Test
    public void test2() {
        TcpClient tcpClient =
                TcpClient.create()
                        .host("example.com")
                        .port(80)
                        .handle((inbound, outbound) -> outbound.sendString(Mono.just("hello")));

        tcpClient.warmup() // 	Initialize and load the event loop group, the host name resolver, the native transport libraries and the native libraries for the security
                .block();

        Connection connection = tcpClient.connectNow(); // Host name resolution happens when connecting to the remote peer

        connection.onDispose()
                .block();
    }

    @Test
    public void test3() {
        Connection connection =
                TcpClient.create()
                        .host("example.com")
                        .port(80)
                        .handle((inbound, outbound) -> outbound.sendString(Mono.just("hello")))
                        .connectNow();

        connection.onDispose()
                .block();
    }

    @Test
    public void test5() {
        Connection connection =
                TcpClient.create()
                        .host("example.com")
                        .port(80)
                        .connectNow();

        connection.outbound()
                .sendString(Mono.just("hello 1"))
                .then()
                .subscribe();

        connection.outbound()
                .sendString(Mono.just("hello 2"))
                .then()
                .subscribe(null, null, connection::dispose);

        connection.onDispose()
                .block();
    }

    @Test
    public void test20() {
        Connection connection =
                TcpClient.create()
                        .host("example.com")
                        .port(80)
                        .handle((inbound, outbound) -> inbound.receive().then())
                        .connectNow();

        connection.onDispose()
                .block();
    }

    @Test
    public void test7() {
        Connection connection =
                TcpClient.create()
                        .host("example.com")
                        .port(80)
                        .connectNow();

        connection.inbound()
                .receive()
                .then()
                .subscribe();

        connection.onDispose()
                .block();
    }
}
