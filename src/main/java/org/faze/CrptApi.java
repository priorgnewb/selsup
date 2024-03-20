package org.faze;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class CrptApi {

    private static final String CREATE_NEW_PRODUCT_DOCUMENT_URL = "http://localhost:8080/documents";
    private static final Queue<RequestWithSign> QUEUE = new ConcurrentLinkedQueue<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int THREAD_POOL_SIZE = 4;
    private static final String TEST_DOCUMENT_JSON = """
            {"description":{"participantInn":"1234567890"},"importRequest":true,"products":[{"certificate_document":"Документ","certificate_document_date":"2024-03-16","certificate_document_number":"HUFL9313H32VATIU763AS","owner_inn":"0987654321","producer_inn":"5555522222","productionDate":"2024-03-16","tnved_code":"GFVSDOOWNWEIHIEHIE32423VBCU","uit_code":"xvxiwe832jpwj932nas0qfoew932obnvoh0sv","uitu_code":"vhw322jrwj932jsdvjv932vwpkp93hv"}],"doc_id":"FG31JN7343LAS85371MNB652A","doc_status":"Утвержден","doc_type":"LP_INTRODUCE_GOODS","owner_inn":"0987654321","participant_inn":"1234567890","producer_inn":"5555522222","production_date":"2024-03-16","production_type":"Цифровая услуга","reg_date":"2024-03-16","reg_number":"35gdsvdsk777ajasal2112alqm782"}""";

    private static volatile AtomicInteger totalDocumentsCount = new AtomicInteger(0);

    private volatile AtomicLong lastResetTime;
    private volatile AtomicInteger requestCount;

    private final long timeLimitMillis;
    private final int requestLimit;
    private final HttpClient HTTP_CLIENT;

    public CrptApi(TimeUnit timeUnit, int requestLimit) throws IOException, URISyntaxException, InterruptedException {
        timeLimitMillis = timeUnit.toMillis(1);
        this.requestLimit = requestLimit;
        lastResetTime = new AtomicLong(Instant.now().toEpochMilli());
        requestCount = new AtomicInteger(0);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        OBJECT_MAPPER.setDateFormat(dateFormat);
        ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        HTTP_CLIENT = HttpClient.newBuilder()
                .executor(threadPool)
                .build();
        CrptApiConsumer taskPusher = new CrptApiConsumer(threadPool);
        taskPusher.startConsumer();
    }

    /**
     * если лимит запросов не превышен - документ отправляется на сохранение сразу,
     * если превышен - документ помещается в очередь
     */
    public void saveDoc(String document, String sign) throws IOException, InterruptedException, URISyntaxException {

        if (isAllowed()) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(CREATE_NEW_PRODUCT_DOCUMENT_URL))
                    .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(document)))
                    .setHeader("Signature", sign)
                    .build();

            HTTP_CLIENT.sendAsync(request, HttpResponse.BodyHandlers.ofString());

            requestCount.incrementAndGet();
            totalDocumentsCount.incrementAndGet();
        } else {
            QUEUE.add(new RequestWithSign(document, sign));
        }
    }

    public boolean isAllowed() {
        if (Instant.now().toEpochMilli() - lastResetTime.get() > timeLimitMillis) {
            lastResetTime.set(Instant.now().toEpochMilli());
            requestCount.set(0);
            return true;
        }

        return requestCount.get() < requestLimit;
    }

    /**
     * постоянно обрабатывает запросы из очереди
     */
    class CrptApiConsumer {
        private final ExecutorService threadPool;

        public CrptApiConsumer(ExecutorService threadPool) {
            this.threadPool = threadPool;
        }

        Runnable task = () -> {
            while (true) {
                if (!QUEUE.isEmpty() && isAllowed()) {
                    RequestWithSign poll = QUEUE.poll();
                    if (poll != null) {
                        try {
                            saveDoc(poll.getDocument(), poll.getSign());
                        } catch (IOException | InterruptedException | URISyntaxException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        };

        void startConsumer() {
            threadPool.execute(task);
        }
    }


    @Data
    @AllArgsConstructor
    static class RequestWithSign {
        String document;
        String sign;
    }


    @Data
    @Builder
    @Jacksonized
    static class Document {
        Description description;
        @JsonProperty("doc_id")
        String docId;
        @JsonProperty("doc_status")
        String docStatus;
        @JsonProperty("doc_type")
        DocType docType;
        boolean importRequest;
        @JsonProperty("owner_inn")
        String ownerINN;
        @JsonProperty("participant_inn")
        String participantINN;
        @JsonProperty("producer_inn")
        String producerINN;
        @JsonProperty("production_date")
        Date productionDate;
        @JsonProperty("production_type")
        String productionType;
        List<Product> products;
        @JsonProperty("reg_date")
        Date regDate;
        @JsonProperty("reg_number")
        String regNumber;
    }

    @Data
    @Builder
    @Jacksonized
    static class Description {
        @JsonProperty("participantInn")
        String participantINN;
    }

    enum DocType {
        LP_INTRODUCE_GOODS,
        LP_INTRODUCE_GOODS_CSV,
        LP_INTRODUCE_GOODS_XML
    }

    @Data
    @Builder
    @Jacksonized
    static class Product {
        @JsonProperty("certificate_document")
        String certificateDocument;
        @JsonProperty("certificate_document_date")
        Date certificateDocumentDate;
        @JsonProperty("certificate_document_number")
        String certificateDocumentNumber;
        @JsonProperty("owner_inn")
        String ownerINN;
        @JsonProperty("producer_inn")
        String producerINN;
        @JsonProperty("productionDate")
        Date productionDate;
        @JsonProperty("tnved_code")
        String tnvedCode;
        @JsonProperty("uit_code")
        String UITCode;
        @JsonProperty("uitu_code")
        String UITUCode;
    }

    /**
     * имитация запросов к API
     */
    public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
        CrptApi crptApi = new CrptApi(TimeUnit.SECONDS, 100);

        for (int i = 0; i < 10_000; i++) {
            crptApi.saveDoc(TEST_DOCUMENT_JSON, "test_sign");
        }
    }
}