package lineage.controller;

import lineage.model.SqlRequest;
import lineage.service.LineageService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@CrossOrigin("*")
public class LineageController {

    private final LineageService lineageService;

    @PostMapping("/lineage")
    public String analyze(@RequestBody SqlRequest request) {
        return lineageService.analyze(request.sql());
    }
}
