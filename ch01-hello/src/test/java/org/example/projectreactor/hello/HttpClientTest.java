package org.example.projectreactor.hello;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.util.CharsetUtil;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClient;
import reactor.netty.tcp.TcpClient;

import java.net.InetSocketAddress;

public class HttpClientTest {

    @Test
    public void test0() {
        HttpClient client = HttpClient.create();

        client.get()
                .uri("https://example.com/")
                .response()
                .block();
    }

    @Test
    public void test1() {
        HttpClient client = HttpClient.create();

        client.websocket()
                .uri("wss://echo.websocket.org")
                .handle((inbound, outbound) -> {
                    inbound.receive()
                            .asString()
                            .take(1)
                            .subscribe(System.out::println);

                    final byte[] msgBytes = "hello".getBytes(CharsetUtil.ISO_8859_1);
                    return outbound.send(Flux.just(Unpooled.wrappedBuffer(msgBytes), Unpooled.wrappedBuffer(msgBytes)))
                            .neverComplete();
                })
                .blockLast();
    }

    @Test
    public void test2() {
        HttpClient client =
                HttpClient.create()
                        .host("example.com")
                        .port(80);

        client.get()
                .uri("/")
                .response()
                .block();
    }

    @Test
    public void test3() {
        HttpClient client = HttpClient.create();

        client.warmup()
                .block();

        client.post()
                .uri("https://example.com/")
                .send(ByteBufFlux.fromString(Mono.just("hello")))
                .response()
                .block();
    }

    @Test
    public void test5() {
        HttpClient client = HttpClient.create();

        client.post()
                .uri("https://example.com/")
                .send(ByteBufFlux.fromString(Mono.just("hello")))
                .response()
                .block();
    }

    @Test
    public void test6() {
        HttpClient client =
                HttpClient.create()
                        .headers(h -> h.set(HttpHeaderNames.CONTENT_LENGTH, 5));

        client.post()
                .uri("https://example.com/")
                .send(ByteBufFlux.fromString(Mono.just("hello")))
                .response()
                .block();
    }

    @Test
    public void test7() {
        HttpClient client =
                HttpClient.create()
                        .compress(true);

        client.get()
                .uri("https://example.com/")
                .response()
                .block();
    }

    @Test
    public void test9() {
        HttpClient client =
                HttpClient.create()
                        .followRedirect(true);

        client.get()
                .uri("https://example.com/")
                .response()
                .block();
    }

    @Test
    public void test10() {
        HttpClient client = HttpClient.create();

        client.get()
                .uri("https://example.com/")
                .responseContent()
                .aggregate()
                .asString()
                .block();
    }

    @Test
    public void test11() {
        HttpClient client = HttpClient.create();

        client.get()
                .uri("https://example.com/")
                .responseSingle((resp, bytes) -> {
                    System.out.println(resp.status());
                    return bytes.asString();
                })
                .block();
    }

    @Test
    public void test20() {
        HttpClient client =
                HttpClient.create()
                        .bindAddress(() -> new InetSocketAddress("host", 1234))
                        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        // The options below are available only when NIO transport (Java 11) is used
                        // on Mac or Linux (Java does not currently support these extended options on Windows)
                        // https://bugs.openjdk.java.net/browse/JDK-8194298
                        //.option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPIDLE), 300)
                        //.option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPINTERVAL), 60)
                        //.option(NioChannelOption.of(ExtendedSocketOptions.TCP_KEEPCOUNT), 8);
                        // The options below are available only when Epoll transport is used
                        .option(EpollChannelOption.TCP_KEEPIDLE, 300)
                        .option(EpollChannelOption.TCP_KEEPINTVL, 60)
                        .option(EpollChannelOption.TCP_KEEPCNT, 8);

        String response =
                client.get()
                        .uri("https://example.com/")
                        .responseContent()
                        .aggregate()
                        .asString()
                        .block();

        System.out.println("Response " + response);
    }

    @Test
    public void test21() {
        HttpClient client =
                HttpClient.newConnection()
                        .doOnConnected(conn -> System.out.println("Connection " + conn.channel()));

        String response =
                // A new connection is established for every request
                client.get()
                        .uri("https://httpbin.org/get")
                        .responseContent()
                        .aggregate()
                        .asString()
                        .block();

        System.out.println("Response " + response);

        response =
                // A new connection is established for every request
                client.post()
                        .uri("https://httpbin.org/post")
                        .responseContent()
                        .aggregate()
                        .asString()
                        .block();

        System.out.println("Response " + response);
    }
}
