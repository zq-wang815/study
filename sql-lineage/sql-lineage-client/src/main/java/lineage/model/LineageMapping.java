package lineage.model;

import lombok.Data;

@Data
public class LineageMapping {
    private String currentField;
    private String upstreamTable;
    private String upstreamFields;
}
