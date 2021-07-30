package org.jumpmind.vaadin.ui.common;

import org.apache.commons.lang3.StringUtils;

import com.vaadin.flow.component.ComponentEvent;
import com.vaadin.flow.component.DomEvent;
import com.vaadin.flow.component.EventData;
import com.vaadin.flow.component.splitlayout.SplitLayout;

@DomEvent("splitter-resized")
public class SplitterResizedEvent extends ComponentEvent<SplitLayout> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final double leftWidth;
    private final double rightWidth;

    /**
     * Creates a new event using the given source and indicator whether the
     * event originated from the client side or the server side.
     *
     * @param source     the source component
     * @param fromClient <code>true</code> if the event originated from the client
     */
    public SplitterResizedEvent(SplitLayout source, boolean fromClient, @EventData("event.detail.leftWidth") String leftWidth
            , @EventData("event.detail.rightWidth") String rightWidth
            )
    {
        super(source, fromClient);

        this.leftWidth = Double.parseDouble(StringUtils.remove(leftWidth, "px"));
        this.rightWidth = Double.parseDouble(StringUtils.remove(rightWidth, "px"));
    }
    
    public double getLeftWidth() {
        return leftWidth;
    }
    
    public double getRightWidth() {
        return rightWidth;
    }
}
