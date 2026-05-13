package lineage.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
public class LineageResult {
    private String currentTable;

    @JsonProperty("lineageMappings")
    private List<LineageMapping> lineageMappings;
}
