package cz.company.guardian;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;

import cz.company.guardian.validation.RejectionReason;
import cz.company.guardian.validation.RequestValidator;
import cz.company.guardian.validation.Violation;

/**
 * Rejects a ProduceRequest, without forwarding anything to the broker, unless every record in it starts with a
 * Confluent frame whose schema ID is whitelisted for the record's topic.
 * <p>
 * Fail closed: the only path that forwards the request is "every record validated successfully".
 * Any exception results in an {@link Errors#INVALID_RECORD} response, or, if even that is impossible,
 * in an exceptionally completed stage, which the proxy answers with an error and never forwards.
 */
final class SchemaGuardianFilter implements ProduceRequestFilter {

    private final GuardianRuntime runtime;

    SchemaGuardianFilter(GuardianRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        try {
            List<Uuid> unresolved = RequestValidator.unresolvedTopicIds(request);
            if (unresolved.isEmpty()) {
                return validateAndDecide(header, request, context, Map.of());
            }
            return context.topicNames(unresolved)
                    .thenCompose(mapping -> onTopicNames(header, request, context, mapping))
                    .exceptionallyCompose(error -> internalError(error, header, request, context));
        }
        catch (RuntimeException | LinkageError | AssertionError | StackOverflowError e) {
            return internalError(e, header, request, context);
        }
    }

    private CompletionStage<RequestFilterResult> onTopicNames(RequestHeaderData header, ProduceRequestData request, FilterContext context,
                                                              TopicNameMapping mapping) {
        if (mapping.anyFailures()) {
            // Same answer the broker would give for an unknown topic ID; nothing is forwarded.
            Violation violation = Violation.of(RejectionReason.TOPIC_NOT_CONFIGURED, null, -1, -1);
            return reject(violation, 0, Errors.UNKNOWN_TOPIC_ID, header, request, context);
        }
        return validateAndDecide(header, request, context, mapping.topicNames());
    }

    private CompletionStage<RequestFilterResult> validateAndDecide(RequestHeaderData header, ProduceRequestData request, FilterContext context,
                                                                   Map<Uuid, String> topicNames) {
        long start = System.nanoTime();
        RequestValidator.Result result = runtime.validator().validate(request, topicNames);
        long elapsed = System.nanoTime() - start;
        if (result.allowed()) {
            runtime.metrics().requestAllowed(result.checkedRecords(), elapsed);
            return context.forwardRequest(header, request);
        }
        return reject(result.violation(), elapsed, Errors.INVALID_RECORD, header, request, context);
    }

    private CompletionStage<RequestFilterResult> reject(Violation violation, long elapsedNanos, Errors error, RequestHeaderData header,
                                                         ProduceRequestData request, FilterContext context) {
        boolean acksZero = request.acks() == 0;
        try {
            runtime.metrics().requestRejected(violation, elapsedNanos, acksZero);
            runtime.rejectLogger().rejected(violation, header.correlationId(), context.sessionId(), context.getVirtualClusterName(), acksZero);
        }
        catch (RuntimeException e) {
            // observability must never change the outcome
        }
        return rejection(header, request, context, error, violation.clientMessage());
    }

    /**
     * With acks=0 the Kafka protocol has no response, so the request is dropped: it cannot be forwarded, and an error
     * response cannot be sent. Everything else gets a response carrying {@code error}.
     */
    private static CompletionStage<RequestFilterResult> rejection(RequestHeaderData header, ProduceRequestData request, FilterContext context, Errors error,
                                                                  String message) {
        if (request.acks() == 0) {
            return context.requestFilterResultBuilder().drop().completed();
        }
        return context.requestFilterResultBuilder().errorResponse(header, request, error, message).completed();
    }

    private CompletionStage<RequestFilterResult> internalError(Throwable error, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        Violation violation = Violation.of(RejectionReason.INTERNAL_VALIDATION_ERROR, null, -1, -1);
        try {
            runtime.metrics().requestRejected(violation, 0, request.acks() == 0);
            runtime.rejectLogger().internalError(cause, header.correlationId(), context.sessionId(), context.getVirtualClusterName());
        }
        catch (RuntimeException e) {
            // observability must never change the outcome
        }
        try {
            return rejection(header, request, context, Errors.INVALID_RECORD, violation.clientMessage());
        }
        catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
