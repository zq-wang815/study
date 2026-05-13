package lineage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class LineageService {

    private final String pythonApiUrl;
    private final ObjectMapper objectMapper;

    public LineageService(@Value("${lineage.api.url:http://localhost:8000}") String pythonApiUrl) {
        this.pythonApiUrl = pythonApiUrl;
        this.objectMapper = new ObjectMapper();
    }

    @SneakyThrows
    public String analyze(String sql) {
        String requestBody = objectMapper.writeValueAsString(java.util.Map.of("sql", sql));

        HttpURLConnection conn = (HttpURLConnection) URI.create(pythonApiUrl + "/sql-lineage").toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.setConnectTimeout(10_000);
        conn.setReadTimeout(300_000);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(requestBody.getBytes(StandardCharsets.UTF_8));
        }

        if (conn.getResponseCode() != 200) {
            String err;
            try (var es = conn.getErrorStream()) {
                err = es != null ? new String(es.readAllBytes(), StandardCharsets.UTF_8) : "no body";
            }
            throw new IOException("Python API " + conn.getResponseCode() + ": " + err);
        }

        return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    }
}
