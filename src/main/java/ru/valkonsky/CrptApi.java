package ru.valkonsky;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class CrptApi implements AutoCloseable {

    public enum Format { JSON, CSV, XML }
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private volatile URI baseUri;
    private final SlidingWindowRateLimiter rateLimiter;

    public static final URI DEFAULT_BASE_URI = URI.create("https://ismp.crpt.ru/api/v3/lk/documents/create");

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        this(timeUnit, requestLimit, DEFAULT_BASE_URI);
    }

    public CrptApi(TimeUnit timeUnit, int requestLimit, URI baseUri) {
        Objects.requireNonNull(timeUnit, "timeUnit");
        Objects.requireNonNull(baseUri, "baseUri");
        if (requestLimit <= 0) throw new IllegalArgumentException("requestLimit must be > 0");

        this.httpClient = HttpClient.newHttpClient();
        this.objectMapper = new ObjectMapper();
        this.baseUri = baseUri;
        long windowMillis = timeUnit.toMillis(1);
        this.rateLimiter = new SlidingWindowRateLimiter(requestLimit, windowMillis);
    }

    public HttpResponse<String> createEntryDocument(Document doc, Format format, String signature)
            throws IOException, InterruptedException {
        Objects.requireNonNull(doc, "document");
        Objects.requireNonNull(format, "format");
        Objects.requireNonNull(signature, "signature");

        rateLimiter.acquire();

        String body;
        String contentType;
        switch (format) {
            case JSON:
                body = serializeDocument(doc);
                contentType = "application/json";
                break;
            case CSV:
                body = buildCsv(doc);
                contentType = "text/csv; charset=utf-8";
                break;
            case XML:
                body = buildXml(doc);
                contentType = "application/xml; charset=utf-8";
                break;
            default:
                throw new IllegalArgumentException("Unsupported format: " + format);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(baseUri)
                .header("Content-Type", contentType)
                .header("X-Signature", signature)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }

    public HttpResponse<String> createEntryDocumentJson(Document doc, String signature) throws IOException, InterruptedException {
        return createEntryDocument(doc, Format.JSON, signature);
    }

    public HttpResponse<String> createEntryDocumentCsv(Document doc, String signature) throws IOException, InterruptedException {
        return createEntryDocument(doc, Format.CSV, signature);
    }

    public HttpResponse<String> createEntryDocumentXml(Document doc, String signature) throws IOException, InterruptedException {
        return createEntryDocument(doc, Format.XML, signature);
    }

    protected String serializeDocument(Object document) throws JsonProcessingException {
        return objectMapper.writeValueAsString(document);
    }

    public void setBaseUri(URI baseUri) {
        Objects.requireNonNull(baseUri, "baseUri");
        this.baseUri = baseUri;
    }

    @Override
    public void close() {

    }

    private String buildCsv(Document d) {
        String header = String.join(",",
                "Тип документа",
                "ИНН участника оборота товаров",
                "Дата производства",
                "ИНН производителя товара",
                "ИНН собственника товаров",
                "Тип производственного заказа",
                "КИ",
                "КИТУ",
                "Код товарной номенклатуры (10 знаков)",
                "Дата производства товара",
                "Документ обязательной сертификации",
                "Номер документа",
                "Дата документа"
        );

        StringBuilder sb = new StringBuilder();
        sb.append(header).append("\n");

        String docType = safeCsv(d.doc_type);
        String participantInn = safeCsv(d.participant_inn != null ? d.participant_inn : (d.description != null ? d.description.participantInn : null));
        String prodDate = safeCsv(d.production_date);
        String producerInn = safeCsv(d.producer_inn);
        String ownerInn = safeCsv(d.owner_inn);
        String productionType = safeCsv(d.production_type);

        if (d.products != null && d.products.length > 0) {
            for (Document.Product p : d.products) {
                String kit = safeCsv(p.uit_code);
                String kitu = safeCsv(p.uitu_code);
                String tnved = safeCsv(p.tnved_code);
                String prodDateProduct = safeCsv(p.production_date != null ? p.production_date : "");
                String certType = safeCsv(p.certificate_document);
                String certNumber = safeCsv(p.certificate_document_number);
                String certDate = safeCsv(p.certificate_document_date);

                sb.append(String.join(",",
                        docType,
                        participantInn,
                        prodDate,
                        producerInn,
                        ownerInn,
                        productionType,
                        kit,
                        kitu,
                        tnved,
                        prodDateProduct,
                        certType,
                        certNumber,
                        certDate
                ));
                sb.append("\n");
            }
        } else {
            sb.append(String.join(",",
                    docType,
                    participantInn,
                    prodDate,
                    producerInn,
                    ownerInn,
                    productionType,
                    "", "", "", "", "", "", ""
            ));
            sb.append("\n");
        }

        return sb.toString();
    }

    private static String safeCsv(String v) {
        if (v == null) return "";
        boolean needQuotes = v.contains(",") || v.contains("\"") || v.contains("\n") || v.contains("\r");
        String s = v.replace("\"", "\"\"");
        return needQuotes ? "\"" + s + "\"" : s;
    }

    private String buildXml(Document d) {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<vvod action_id=\"05\" version=\"5\">\n");
        String tradeInn = d.participant_inn != null ? d.participant_inn : (d.description != null ? d.description.participantInn : null);
        appendTag(sb, "trade_participant_inn", tradeInn);
        appendTag(sb, "producer_inn", d.producer_inn);
        appendTag(sb, "owner_inn", d.owner_inn);

        if (d.production_date != null) {
            appendTag(sb, "product_date", d.production_date);
        }

        if (d.production_type != null) {
            appendTag(sb, "production_order", d.production_type);
        }
        sb.append("  <products_list>\n");
        if (d.products != null) {
            for (Document.Product p : d.products) {
                sb.append("    <product>\n");
                if (p.uit_code != null) appendTagIndented(sb, "kit", p.uit_code, 6);   // КИ
                if (p.uitu_code != null) appendTagIndented(sb, "kitu", p.uitu_code, 6); // КИТУ
                if (p.production_date != null) appendTagIndented(sb, "product_date", p.production_date, 6);
                if (p.tnved_code != null) appendTagIndented(sb, "tnved_code", p.tnved_code, 6);
                if (p.certificate_document != null) appendTagIndented(sb, "certificate_type", p.certificate_document, 6);
                if (p.certificate_document_number != null) appendTagIndented(sb, "certificate_number", p.certificate_document_number, 6);
                if (p.certificate_document_date != null) appendTagIndented(sb, "certificate_date", p.certificate_document_date, 6);
                sb.append("    </product>\n");
            }
        }
        sb.append("  </products_list>\n");
        sb.append("</vvod>\n");
        return sb.toString();
    }

    private static void appendTag(StringBuilder sb, String tag, String value) {
        if (value == null) return;
        sb.append("  <").append(tag).append(">");
        sb.append(escapeXml(value));
        sb.append("</").append(tag).append(">\n");
    }

    private static void appendTagIndented(StringBuilder sb, String tag, String value, int indentSpaces) {
        if (value == null) return;
        for (int i = 0; i < indentSpaces; i++) sb.append(' ');
        sb.append("<").append(tag).append(">");
        sb.append(escapeXml(value));
        sb.append("</").append(tag).append(">\n");
    }

    private static String escapeXml(String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder(s.length());
        for (char c : s.toCharArray()) {
            switch (c) {
                case '&': out.append("&amp;"); break;
                case '<': out.append("&lt;"); break;
                case '>': out.append("&gt;"); break;
                case '\"': out.append("&quot;"); break;
                case '\'': out.append("&apos;"); break;
                default: out.append(c);
            }
        }
        return out.toString();
    }

    private static final class SlidingWindowRateLimiter {
        private final int limit;
        private final long windowMillis;
        private final Deque<Long> timestamps;
        private final ReentrantLock lock;
        private final Condition notFull;

        SlidingWindowRateLimiter(int limit, long windowMillis) {
            this.limit = limit;
            this.windowMillis = windowMillis;
            this.timestamps = new ArrayDeque<>(limit + 2);
            this.lock = new ReentrantLock(true);
            this.notFull = lock.newCondition();
        }

        void acquire() throws InterruptedException {
            lock.lock();
            try {
                while (true) {
                    long now = nowMillis();
                    purgeOld(now);
                    if (timestamps.size() < limit) {
                        timestamps.addLast(now);
                        notFull.signalAll();
                        return;
                    } else {
                        long oldest = timestamps.peekFirst();
                        long waitUntil = oldest + windowMillis;
                        long waitMs = waitUntil - now;
                        if (waitMs <= 0) {
                            // старые записи уже вышли — повторим проверку
                            purgeOld(now);
                            continue;
                        }
                        notFull.awaitNanos(TimeUnit.MILLISECONDS.toNanos(waitMs));
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        private void purgeOld(long nowMillis) {
            long threshold = nowMillis - windowMillis;
            while (!timestamps.isEmpty() && timestamps.peekFirst() <= threshold) {
                timestamps.removeFirst();
            }
        }

        private long nowMillis() {
            return Instant.now().toEpochMilli();
        }
    }

    public static class Document {
        public Description description;
        public String doc_id;
        public String doc_status;
        public String doc_type; // LP_INTRODUCE_GOODS
        public Boolean importRequest;
        public String owner_inn;
        public String participant_inn;
        public String producer_inn;
        public String production_date; // YYYY-MM-DD
        public String production_type; // OWN_PRODUCTION / CONTRACT_PRODUCTION
        public Product[] products;
        public String reg_date; // YYYY-MM-DDTHH:mm:ss
        public String reg_number;

        public static class Description {
            public String participantInn;
        }

        public static class Product {
            public String certificate_document; // CONFORMITY_CERTIFICATE / CONFORMITY_DECLARATION
            public String certificate_document_date; // YYYY-MM-DD
            public String certificate_document_number;
            public String owner_inn;
            public String producer_inn;
            public String production_date; // YYYY-MM-DD
            public String tnved_code; // 10 digits
            public String uit_code; // КИ
            public String uitu_code; // КИТУ
        }
    }

    public static void main(String[] args) throws Exception {
        // Пример: 10 запросов в минуту
        CrptApi api = new CrptApi(TimeUnit.MINUTES, 10);

        Document doc = new Document();
        doc.doc_id = "doc-0001";
        doc.doc_status = "NEW";
        doc.doc_type = "LP_INTRODUCE_GOODS";
        doc.owner_inn = "7700000000";
        doc.participant_inn = "7700000000";
        doc.producer_inn = "7700000000";
        doc.production_date = "2025-08-10";
        doc.production_type = "OWN_PRODUCTION";
        doc.description = new Document.Description();
        doc.description.participantInn = "7700000000";

        Document.Product p1 = new Document.Product();
        p1.uit_code = "11111111111111111111111111111111111111"; // КИ
        p1.uitu_code = "000000000000000000"; // КИТУ
        p1.tnved_code = "6401921000";
        p1.production_date = "2025-08-10";
        p1.certificate_document = "CONFORMITY_CERTIFICATE";
        p1.certificate_document_number = "CERT-1";
        p1.certificate_document_date = "2025-08-10";

        Document.Product p2 = new Document.Product();
        p2.uit_code = "22222222222222222222222222222222222222";
        p2.uitu_code = "111111111111111111";
        p2.tnved_code = "8711209200";
        p2.production_date = "2025-08-10";
        p2.certificate_document = "CONFORMITY_DECLARATION";
        p2.certificate_document_number = "DECL-1";
        p2.certificate_document_date = "2025-08-10";

        doc.products = new Document.Product[]{p1, p2};

        String signature = "BASE64_SIGNATURE_SAMPLE";

        HttpResponse<String> rJson = api.createEntryDocumentJson(doc, signature);
        System.out.println("[JSON] status=" + rJson.statusCode());
        System.out.println("[JSON] body=" + rJson.body());

        HttpResponse<String> rCsv = api.createEntryDocumentCsv(doc, signature);
        System.out.println("[CSV] status=" + rCsv.statusCode());
        System.out.println("[CSV] body=" + rCsv.body());

        HttpResponse<String> rXml = api.createEntryDocumentXml(doc, signature);
        System.out.println("[XML] status=" + rXml.statusCode());
        System.out.println("[XML] body=" + rXml.body());
    }
}