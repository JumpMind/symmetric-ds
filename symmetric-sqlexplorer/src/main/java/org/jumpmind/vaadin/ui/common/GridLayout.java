package org.jumpmind.vaadin.ui.common;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.orderedlayout.FlexComponent.Alignment;
import com.vaadin.flow.component.orderedlayout.FlexComponent.JustifyContentMode;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;

public class GridLayout extends Div {

    private static final long serialVersionUID = 1L;

    protected VerticalLayout layout;
    
    protected List<Row> rowList;
    
    protected List<Float> columnExpandRatioList;
    
    protected int columnCount;
    
    protected int rowCount;
    
    protected int cursorColumnIndex;
    
    protected int cursorRowIndex;
    
    protected Alignment defaultVerticalAlignment;
    
    protected Alignment defaultHorizontalAlignment;
    
    protected boolean spacing = true;
    
    public GridLayout() {
        this(1, 1);
    }
    
    public GridLayout(int columnCount, int rowCount) {
        layout = new VerticalLayout();
        rowList = new ArrayList<Row>();
        columnExpandRatioList = new ArrayList<Float>(columnCount);
        this.columnCount = columnCount > 0 ? columnCount : 1;
        this.rowCount = rowCount > 0 ? rowCount : 1;
        cursorColumnIndex = 0;
        cursorRowIndex = 0;
        
        for (int i = 0; i < rowCount; i++) {
            addRow();
        }
        
        for (int i = 0; i < columnCount; i++) {
            columnExpandRatioList.add(0f);
        }
        
        layout.setSizeFull();
        super.add(layout);
    }
    
    public void add(Component component) {
        if (cursorRowIndex >= rowCount) {
            addRow();
            rowCount++;
        }
        
        Row row = rowList.get(cursorRowIndex);
        if (row.getArea(cursorColumnIndex).isEmpty()) {
            row.setArea(new Area(component, 1, cursorColumnIndex));
            incrementCursor();
        } else {
            incrementCursor();
            add(component);
        }
    }
    
    public void add(Component component, int width) {
        if (width > 0 && width <= columnCount) {
            Row row = rowList.get(cursorRowIndex);
            int firstValidColumnIndex = 0;
            int validColumns = 0;
            
            for (int i = cursorColumnIndex; i < columnCount; i++) {
                if (row.getArea(i).isEmpty()) {
                    if (validColumns == 0) {
                        firstValidColumnIndex = i;
                    }
                    validColumns++;
                    if (validColumns == width) {
                        break;
                    }
                } else {
                    validColumns = 0;
                }
            }
            
            if (validColumns == width) {
                row.setArea(new Area(component, width, firstValidColumnIndex));
            } else {
                if (cursorRowIndex == rowCount - 1) {
                    addRow();
                    rowCount++;
                }
                cursorColumnIndex = 0;
                cursorRowIndex++;
                add(component, width);
            }
        }
    }
    
    public void add(Component component, int columnIndex, int rowIndex) {
        if (columnIndex >= 0 && columnIndex < columnCount && rowIndex >= 0 && rowIndex < rowCount) {
            Row row = rowList.get(rowIndex);
            if (row.getArea(columnIndex).isEmpty()) {
                row.setArea(new Area(component, 1, columnIndex));
            }
        }
    }
    
    public void add(Component component, int firstColumnIndex, int lastColumnIndex, int rowIndex) {
        if (firstColumnIndex <= lastColumnIndex && firstColumnIndex >= 0 && lastColumnIndex < columnCount
                && rowIndex >= 0 && rowIndex < rowCount) {
            Row row = rowList.get(rowIndex);
            boolean valid = true;
            for (int i = firstColumnIndex; i <= lastColumnIndex; i++) {
                if (!row.getArea(i).isEmpty()) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                row.setArea(new Area(component, 1 + lastColumnIndex - firstColumnIndex, firstColumnIndex));
            }
        }
    }
    
    @Override
    public void removeAll() {
        layout.removeAll();
        
        cursorColumnIndex = 0;
        cursorRowIndex = 0;
        
        rowList.clear();
        for (int i = 0; i < rowCount; i++) {
            addRow();
        }
        
        columnExpandRatioList.clear();
        for (int i = 0; i < columnCount; i++) {
            columnExpandRatioList.add(0f);
        }
    }
    
    public void setColumnExpandRatio(int columnIndex, float ratio) {
        if (columnIndex < columnCount && ratio >= 0f) {
            columnExpandRatioList.set(columnIndex, ratio);
            
            for (Row row : rowList) {
                float expandRatio = 0f;
                for (int i = 0; i < columnCount; i++) {
                    Area area = row.getArea(i);
                    expandRatio += columnExpandRatioList.get(i);
                    if (i == area.getColumnCount() + area.getFirstColumnIndex() - 1) {
                        row.setFlexGrow(expandRatio, area);
                        expandRatio = 0;
                    }
                }
            }
        }
    }
    
    public void setRowExpandRatio(int rowIndex, float ratio) {
        if (rowIndex < rowCount && ratio >= 0f) {
            layout.setFlexGrow(ratio, rowList.get(rowIndex));
        }
    }
    
    public void setComponentAlignment(Component component, Alignment verticalAlignment, Alignment horizontalAlignment) {
        if (component != null) {
            for (Row row : rowList) {
                for (int i = 0; i < columnCount; i++) {
                    Area area = row.getArea(i);
                    Component areaComponent = area.getComponent();
                    if (component.equals(areaComponent)) {
                        area.setHorizontalComponentAlignment(horizontalAlignment, areaComponent);
                        switch (verticalAlignment) {
                            case START:
                                area.setJustifyContentMode(JustifyContentMode.START);
                                break;
                            case CENTER:
                                area.setJustifyContentMode(JustifyContentMode.CENTER);
                                break;
                            case END:
                                area.setJustifyContentMode(JustifyContentMode.END);
                            default:
                        }
                        area.setDefaultAlignment(false);
                    }
                }
            }
        }
    }
    
    public void setDefaultComponentAlignment(Alignment verticalAlignment, Alignment horizontalAlignment) {
        defaultVerticalAlignment = verticalAlignment;
        defaultHorizontalAlignment = horizontalAlignment;
        
        for (Row row : rowList) {
            for (Component child : row.getChildren().collect(Collectors.toList())) {
                Area area = (Area) child;
                Component areaComponent = area.getComponent();
                if (areaComponent != null && area.isAlignmentDefault()) {
                    area.setHorizontalComponentAlignment(horizontalAlignment, areaComponent);
                    switch (verticalAlignment) {
                        case START:
                            area.setJustifyContentMode(JustifyContentMode.START);
                            break;
                        case CENTER:
                            area.setJustifyContentMode(JustifyContentMode.CENTER);
                            break;
                        case END:
                            area.setJustifyContentMode(JustifyContentMode.END);
                        default:
                    }
                }
            }
        }
    }
    
    public void setSpacing(boolean spacing) {
        this.spacing = spacing;
        getElement().getThemeList().set("spacing", spacing);
        for (Row row : rowList) {
            row.setSpacing(spacing);
            for (Area area : row.areaList) {
                area.setSpacing(spacing);
            }
        }
    }
    
    private void addRow() {
        Row row = new Row(columnCount);
        rowList.add(row);
        layout.add(row);
    }
    
    private void incrementCursor() {
        if (cursorColumnIndex < columnCount - 1) {
            cursorColumnIndex++;
        } else {
            cursorColumnIndex = 0;
            cursorRowIndex++;
        }
    }
    
    protected class Row extends HorizontalLayout {
        
        private static final long serialVersionUID = 1L;
        
        private List<Area> areaList;
        
        public Row(int columnCount) {
            super();
            setWidthFull();
            setSpacing(spacing);
            
            areaList = new ArrayList<Area>();
            for (int i = 0; i < columnCount; i++) {
                Area area = new Area(i);
                areaList.add(area);
                add(area);
            }
        }
        
        public Area getArea(int columnIndex) {
            if (columnIndex < areaList.size()) {
                return areaList.get(columnIndex);
            }
            return null;
        }
        
        public void setArea(Area area) {
            int firstColumnIndex = area.getFirstColumnIndex();
            int columnCount = area.getColumnCount();
            int childIndex = indexOf(areaList.get(firstColumnIndex));
            float expandRatio = 0f;
            
            for (int i = firstColumnIndex; i < firstColumnIndex + columnCount; i++) {
                remove(areaList.get(i));
                areaList.set(i, area);
                expandRatio += columnExpandRatioList.get(i);
            }
            
            addComponentAtIndex(childIndex, area);
            setFlexGrow(expandRatio, area);
        }
    }
    
    protected class Area extends VerticalLayout {

        private static final long serialVersionUID = 1L;

        private Component component;
        
        private int columnCount;
        
        private int firstColumnIndex;
        
        private boolean defaultAlignment;
        
        public Area(int firstColumn) {
            this(null, 1, firstColumn);
        }
        
        public Area(Component component, int columnCount, int firstColumnIndex) {
            this.component = component;
            this.columnCount = columnCount;
            this.firstColumnIndex = firstColumnIndex;
            this.defaultAlignment = true;
            setWidth(null);
            setSpacing(spacing);
            
            if (component != null) {
                add(component);
                if (defaultVerticalAlignment != null) {
                    setHorizontalComponentAlignment(defaultHorizontalAlignment);
                    switch (defaultVerticalAlignment) {
                        case START:
                            setJustifyContentMode(JustifyContentMode.START);
                            break;
                        case CENTER:
                            setJustifyContentMode(JustifyContentMode.CENTER);
                            break;
                        case END:
                            setJustifyContentMode(JustifyContentMode.END);
                        default:
                    }
                }
            }
        }
        
        public boolean isEmpty() {
            return component == null;
        }
        
        public Component getComponent() {
            return component;
        }

        public int getColumnCount() {
            return columnCount;
        }

        public int getFirstColumnIndex() {
            return firstColumnIndex;
        }
        
        public void setFirstColumnIndex(int firstColumnIndex) {
            this.firstColumnIndex = firstColumnIndex;
        }
        
        public boolean isAlignmentDefault() {
            return defaultAlignment;
        }
        
        public void setDefaultAlignment(boolean defaultAlignment) {
            this.defaultAlignment = defaultAlignment;
        }
        
    }
    
}
