/*
 * Copyright © 2018 Knative Authors (knative-dev@googlegroups.com)
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
 * limitations under the License.
 */
package dev.knative.eventing.kafka.broker.dispatcher.impl.consumer;

import static dev.knative.eventing.kafka.broker.core.testing.CoreObjects.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.knative.eventing.kafka.broker.contract.DataPlaneContract;
import dev.knative.eventing.kafka.broker.core.observability.metrics.Metrics;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSender;
import dev.knative.eventing.kafka.broker.dispatcher.CloudEventSenderMock;
import dev.knative.eventing.kafka.broker.dispatcher.MockReactiveKafkaConsumer;
import dev.knative.eventing.kafka.broker.dispatcher.RecordDispatcher;
import dev.knative.eventing.kafka.broker.dispatcher.ResponseHandlerMock;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherImpl;
import dev.knative.eventing.kafka.broker.dispatcher.impl.RecordDispatcherTest;
import dev.knative.eventing.kafka.broker.dispatcher.main.ConsumerVerticleContext;
import dev.knative.eventing.kafka.broker.dispatcher.main.FakeConsumerVerticleContext;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.backends.BackendRegistries;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public abstract class AbstractConsumerVerticleTest {

    private static final ConsumerVerticleContext resourceContext = FakeConsumerVerticleContext.get();

    static {
        BackendRegistries.setupBackend(
                new MicrometerMetricsOptions().setRegistryName(Metrics.METRICS_REGISTRY_NAME), null);
    }

    @Test
    public void subscribedToTopic(final Vertx vertx, final VertxTestContext context) {
        final var consumer = new MockConsumer<Object, CloudEvent>(OffsetResetStrategy.LATEST);
        final var recordDispatcher = new RecordDispatcherImpl(
                resourceContext,
                value -> false,
                CloudEventSender.noop("subscriber send called"),
                CloudEventSender.noop("dead letter sink send called"),
                new ResponseHandlerMock(),
                RecordDispatcherTest.offsetManagerMock(),
                null,
                Metrics.getRegistry());
        final var topic = "topic1";

        final var consumerVerticleContext = FakeConsumerVerticleContext.get(
                DataPlaneContract.Resource.newBuilder(resource1())
                        .clearTopics()
                        .addTopics(topic)
                        .build(),
                egress1());

        final var verticle = createConsumerVerticle(consumerVerticleContext, (vx, consumerVerticle) -> {
            consumerVerticle.setConsumer(new MockReactiveKafkaConsumer<>(consumer));
            consumerVerticle.setRecordDispatcher(recordDispatcher);
            consumerVerticle.setCloser(Future::succeededFuture);
            consumerVerticle.setRebalanceListener(new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {}
            });
            return Future.succeededFuture();
        });

        final Promise<String> promise = Promise.promise();
        vertx.deployVerticle(verticle, promise);

        promise.future()
                .onSuccess(ignored -> context.verify(() -> {
                    assertThat(consumer.subscription()).containsExactlyInAnyOrder(topic);
                    assertThat(consumer.closed()).isFalse();
                    context.completeNow();
                }))
                .onFailure(Assertions::fail);
    }

    @Test
    public void stop(final Vertx vertx, final VertxTestContext context) {
        final var consumer = new MockConsumer<Object, CloudEvent>(OffsetResetStrategy.LATEST);
        final var recordDispatcher = new RecordDispatcherImpl(
                resourceContext,
                value -> false,
                CloudEventSender.noop("subscriber send called"),
                CloudEventSender.noop("dead letter sink send called"),
                new ResponseHandlerMock(),
                RecordDispatcherTest.offsetManagerMock(),
                null,
                Metrics.getRegistry());
        final var topic = "topic1";

        final var verticle = createConsumerVerticle(
                FakeConsumerVerticleContext.get(
                        DataPlaneContract.Resource.newBuilder(resource1())
                                .clearTopics()
                                .addTopics(topic)
                                .build(),
                        egress1()),
                (vx, consumerVerticle) -> {
                    consumerVerticle.setConsumer(new MockReactiveKafkaConsumer<>(consumer));
                    consumerVerticle.setRecordDispatcher(recordDispatcher);
                    consumerVerticle.setCloser(Future::succeededFuture);
                    consumerVerticle.setRebalanceListener(new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {}

                        @Override
                        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {}
                    });

                    return Future.succeededFuture();
                });

        final Promise<String> deployPromise = Promise.promise();
        vertx.deployVerticle(verticle, deployPromise);

        final Promise<Void> undeployPromise = Promise.promise();

        deployPromise
                .future()
                .onSuccess(deploymentID -> vertx.undeploy(deploymentID, undeployPromise))
                .onFailure(context::failNow);

        undeployPromise.future().onSuccess(ignored -> {}).onFailure(context::failNow);

        await().untilAsserted(() -> assertThat(consumer.closed()).isTrue());
        context.completeNow();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCloseEverything(final Vertx vertx, final VertxTestContext context) {
        final var topics = new String[] {"a"};
        final MockReactiveKafkaConsumer<Object, CloudEvent> consumer = mock(MockReactiveKafkaConsumer.class);

        final var checkpoints = context.checkpoint(2);

        when(consumer.close()).thenReturn(Future.succeededFuture());
        when(consumer.unsubscribe()).thenReturn(Future.succeededFuture());
        when(consumer.subscribe((Set<String>) any(), any(ConsumerRebalanceListener.class)))
                .thenReturn(Future.succeededFuture());
        when(consumer.subscribe(any(Set.class))).thenReturn(Future.succeededFuture());
        when(consumer.poll(any())).then(answer -> {
            final Duration duration = answer.getArgument(0);
            Promise<Void> promise = Promise.promise();
            vertx.setTimer(duration.toMillis(), v -> promise.complete());
            return promise.future();
        });

        final var mockConsumer = new MockConsumer<Object, CloudEvent>(OffsetResetStrategy.LATEST);
        when(consumer.unwrap()).thenReturn(mockConsumer);

        mockConsumer.schedulePollTask(() -> {
            mockConsumer.unsubscribe();

            mockConsumer.assign(Arrays.stream(topics)
                    .map(topic -> new TopicPartition(topic, 0))
                    .collect(Collectors.toSet()));
        });

        final var consumerRecordSenderClosed = new AtomicBoolean(false);
        final var dlsSenderClosed = new AtomicBoolean(false);
        final var sinkClosed = new AtomicBoolean(false);

        final var recordDispatcher = new RecordDispatcherImpl(
                resourceContext,
                ce -> true,
                new CloudEventSenderMock(record -> Future.succeededFuture(), () -> {
                    consumerRecordSenderClosed.set(true);
                    return Future.succeededFuture();
                }),
                new CloudEventSenderMock(record -> Future.succeededFuture(), () -> {
                    dlsSenderClosed.set(true);
                    return Future.succeededFuture();
                }),
                new ResponseHandlerMock(response -> Future.succeededFuture(), () -> {
                    sinkClosed.set(true);
                    return Future.succeededFuture();
                }),
                RecordDispatcherTest.offsetManagerMock(),
                null,
                Metrics.getRegistry());

        final var verticle = createConsumerVerticle(FakeConsumerVerticleContext.get(), (vx, consumerVerticle) -> {
            consumerVerticle.setConsumer(consumer);
            consumerVerticle.setRecordDispatcher(recordDispatcher);
            consumerVerticle.setCloser(Future::succeededFuture);
            consumerVerticle.setRebalanceListener(new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {}
            });
            return Future.succeededFuture();
        });

        vertx.deployVerticle(verticle).onFailure(context::failNow).onSuccess(r -> vertx.undeploy(r)
                .onFailure(context::failNow)
                .onSuccess(ignored -> context.verify(() -> {
                    assertThat(consumerRecordSenderClosed.get()).isTrue();
                    assertThat(sinkClosed.get()).isTrue();
                    assertThat(dlsSenderClosed.get()).isTrue();
                    checkpoints.flag();
                })));

        await().untilAsserted(() -> {
            verify(consumer, times(1)).unsubscribe();
            verify(consumer, times(1)).close();
        });
        checkpoints.flag();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unsubscribeOccursBeforeDispatcherDrain(final Vertx vertx, final VertxTestContext context) {
        final var unsubscribedBeforeDrain = new AtomicBoolean(false);

        final MockReactiveKafkaConsumer<Object, CloudEvent> consumer = mock(MockReactiveKafkaConsumer.class);
        when(consumer.unsubscribe()).thenAnswer(inv -> {
            unsubscribedBeforeDrain.set(true);
            return Future.succeededFuture();
        });
        when(consumer.close()).thenReturn(Future.succeededFuture());
        when(consumer.subscribe((Set<String>) any(), any(ConsumerRebalanceListener.class)))
                .thenReturn(Future.succeededFuture());
        when(consumer.subscribe(any(Set.class))).thenReturn(Future.succeededFuture());
        when(consumer.poll(any())).then(answer -> {
            final Duration duration = answer.getArgument(0);
            Promise<Void> promise = Promise.promise();
            vertx.setTimer(duration.toMillis(), v -> promise.complete());
            return promise.future();
        });
        when(consumer.unwrap()).thenReturn(new MockConsumer<>(OffsetResetStrategy.LATEST));

        final var drainCalled = new AtomicBoolean(false);
        final RecordDispatcher recordDispatcher = new RecordDispatcher() {
            @Override
            public Future<Void> dispatch(ConsumerRecord<Object, CloudEvent> record) {
                return Future.succeededFuture();
            }

            @Override
            public Future<Void> close() {
                drainCalled.set(true);
                context.verify(() -> assertThat(unsubscribedBeforeDrain.get())
                        .as("unsubscribe() must be called before the dispatcher drain begins")
                        .isTrue());
                return Future.succeededFuture();
            }
        };

        final var verticle = createConsumerVerticle(FakeConsumerVerticleContext.get(), (vx, cv) -> {
            cv.setConsumer(consumer);
            cv.setRecordDispatcher(recordDispatcher);
            cv.setCloser(Future::succeededFuture);
            cv.setRebalanceListener(new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {}
            });
            return Future.succeededFuture();
        });

        vertx.deployVerticle(verticle)
                .onSuccess(id -> vertx.undeploy(id)
                        .onSuccess(ignored -> context.verify(() -> {
                            assertThat(drainCalled.get()).isTrue();
                            context.completeNow();
                        }))
                        .onFailure(context::failNow))
                .onFailure(context::failNow);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void failedUnsubscribeFallsBackToImmediateClose(final Vertx vertx, final VertxTestContext context) {
        final var closeCalled = new AtomicBoolean(false);
        final Promise<Void> drainGate = Promise.promise();

        final MockReactiveKafkaConsumer<Object, CloudEvent> consumer = mock(MockReactiveKafkaConsumer.class);
        when(consumer.unsubscribe()).thenReturn(Future.failedFuture("simulated unsubscribe failure"));
        when(consumer.close()).thenAnswer(inv -> {
            closeCalled.set(true);
            return Future.succeededFuture();
        });
        when(consumer.subscribe((Set<String>) any(), any(ConsumerRebalanceListener.class)))
                .thenReturn(Future.succeededFuture());
        when(consumer.subscribe(any(Set.class))).thenReturn(Future.succeededFuture());
        when(consumer.poll(any())).then(answer -> {
            final Duration duration = answer.getArgument(0);
            Promise<Void> promise = Promise.promise();
            vertx.setTimer(duration.toMillis(), v -> promise.complete());
            return promise.future();
        });
        when(consumer.unwrap()).thenReturn(new MockConsumer<>(OffsetResetStrategy.LATEST));

        final RecordDispatcher slowDrain = new RecordDispatcher() {
            @Override
            public Future<Void> dispatch(ConsumerRecord<Object, CloudEvent> record) {
                return Future.succeededFuture();
            }

            @Override
            public Future<Void> close() {
                return drainGate.future();
            }
        };

        final var verticle = createConsumerVerticle(FakeConsumerVerticleContext.get(), (vx, cv) -> {
            cv.setConsumer(consumer);
            cv.setRecordDispatcher(slowDrain);
            cv.setCloser(Future::succeededFuture);
            cv.setRebalanceListener(new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {}

                @Override
                public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {}
            });
            return Future.succeededFuture();
        });

        final Promise<String> deployPromise = Promise.promise();
        vertx.deployVerticle(verticle, deployPromise);

        final Promise<Void> undeployPromise = Promise.promise();
        deployPromise
                .future()
                .onSuccess(id -> vertx.undeploy(id, undeployPromise))
                .onFailure(context::failNow);
        undeployPromise.future().onFailure(context::failNow);

        // close() must be called before the drain gate is released
        await().until(closeCalled::get);
        assertThat(drainGate.future().isComplete())
                .as("consumer.close() must be called before the dispatcher drain completes")
                .isFalse();

        drainGate.complete();
        await().until(() -> undeployPromise.future().isComplete());
        context.completeNow();
    }

    abstract ConsumerVerticle createConsumerVerticle(
            final ConsumerVerticleContext context, final ConsumerVerticle.Initializer initializer);
}
