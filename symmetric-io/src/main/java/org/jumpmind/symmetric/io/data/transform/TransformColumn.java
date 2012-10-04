package org.jumpmind.symmetric.io.data.transform;

public class TransformColumn {

    public enum IncludeOnType {
        INSERT, UPDATE, DELETE, ALL;
        public static IncludeOnType decode(String v) {
            if (v.equals("I")) {
                return INSERT;
            } else if (v.equals("U")) {
                return UPDATE;
            } else if (v.equals("D")) {
                return DELETE;
            } else {
                return ALL;
            }
        };

        public String toDbValue() {
            if (this == INSERT) {
                return "I";
            } else if (this == DELETE) {
                return "D";
            } else if (this == UPDATE) {
                return "U";
            } else {
                return "*";
            }
        }

    }

    protected String transformId;
    protected String sourceColumnName;
    protected String targetColumnName;
    protected boolean pk;
    protected String transformType = CopyColumnTransform.NAME;
    protected String transformExpression;
    protected int transformOrder;
    protected IncludeOnType includeOn = IncludeOnType.ALL;

    public TransformColumn(String transformId) {
        this.transformId = transformId;
    }

    public TransformColumn() {
    }

    public TransformColumn(String sourceColumnName, String targetColumnName, boolean pk) {
        this.sourceColumnName = sourceColumnName;
        this.targetColumnName = targetColumnName;
        this.pk = pk;
    }

    public TransformColumn(String sourceColumnName, String targetColumnName, boolean pk,
            String transformType, String transformExpression) {
        this(sourceColumnName, targetColumnName, pk);
        this.transformType = transformType;
        this.transformExpression = transformExpression;
    }

    public String getSourceColumnName() {
        return sourceColumnName;
    }

    public void setSourceColumnName(String sourceColumnName) {
        this.sourceColumnName = sourceColumnName;
    }

    public String getTargetColumnName() {
        return targetColumnName;
    }

    public void setTargetColumnName(String targetColumnName) {
        this.targetColumnName = targetColumnName;
    }

    public boolean isPk() {
        return pk;
    }

    public void setPk(boolean pk) {
        this.pk = pk;
    }

    public String getTransformType() {
        return transformType;
    }

    public void setTransformType(String transformType) {
        this.transformType = transformType;
    }

    public String getTransformExpression() {
        return transformExpression;
    }

    public void setTransformExpression(String transformExpression) {
        this.transformExpression = transformExpression;
    }

    public void setIncludeOn(IncludeOnType includeOn) {
        this.includeOn = includeOn;
    }

    public IncludeOnType getIncludeOn() {
        return includeOn;
    }

    public void setTransformOrder(int transformOrder) {
        this.transformOrder = transformOrder;
    }

    public int getTransformOrder() {
        return transformOrder;
    }

    public void setTransformId(String transformId) {
        this.transformId = transformId;
    }

    public String getTransformId() {
        return transformId;
    }
}
