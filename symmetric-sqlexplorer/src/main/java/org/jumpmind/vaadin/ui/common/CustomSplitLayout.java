package org.jumpmind.vaadin.ui.common;

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.splitlayout.SplitLayout;

public class CustomSplitLayout extends SplitLayout {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private static final String PRIMARY_OFFSET_HEIGHT = "element._primaryChild.offsetHeight";
    private static final String SECONDARY_OFFSET_HEIGHT = "element._secondaryChild.offsetHeight";
    private static final String PRIMARY_OFFSET_WIDTH = "element._primaryChild.offsetWidth";
    private static final String SECONDARY_OFFSET_WIDTH = "element._secondaryChild.offsetWidth";

    private String PRIMARY_SIZE = PRIMARY_OFFSET_WIDTH;
    private String SECONDARY_SIZE = SECONDARY_OFFSET_WIDTH;

    private double splitterPosition;
    private double primarySizePixel;
    private double secondarySizePixel;

    public CustomSplitLayout() {
        super();
        init();
    }
    
    public CustomSplitLayout(Component primaryComponent, Component secondaryComponent) {
        super(primaryComponent, secondaryComponent);
        init();
    }
    
    private void init() {
        getElement().addEventListener("splitter-dragend", e -> {
            this.primarySizePixel = e.getEventData().getNumber(PRIMARY_SIZE);
            this.secondarySizePixel = e.getEventData().getNumber(SECONDARY_SIZE);

            double totalSize = this.primarySizePixel + this.secondarySizePixel;
            this.splitterPosition = Math.round((this.primarySizePixel / totalSize) * 100.0);
        }).addEventData(PRIMARY_OFFSET_WIDTH).addEventData(PRIMARY_OFFSET_HEIGHT)
                .addEventData(SECONDARY_OFFSET_WIDTH).addEventData(SECONDARY_OFFSET_HEIGHT);
    }
    
    @Override
    public void setOrientation(Orientation orientation) {
      super.setOrientation(orientation);

      if (orientation == Orientation.HORIZONTAL) {
        PRIMARY_SIZE = PRIMARY_OFFSET_WIDTH;
        SECONDARY_SIZE = SECONDARY_OFFSET_WIDTH;
      } else {
        PRIMARY_SIZE = PRIMARY_OFFSET_HEIGHT;
        SECONDARY_SIZE = SECONDARY_OFFSET_HEIGHT;
      }
    }

    public double getSplitterPosition() {
        return splitterPosition;
      }
    
    @Override
    public void setSplitterPosition(double position) {
        super.setSplitterPosition(position);

        this.splitterPosition = position;
    }
    
    public double getPrimarySizePixel() {
        return primarySizePixel;
    }

    public double getSecondarySizePixel() {
        return secondarySizePixel;
    }
}
