package org.jumpmind.vaadin.ui.common;

import java.util.Locale;

import org.apache.commons.lang.StringUtils;

import com.vaadin.v7.data.util.converter.Converter;

public class AbbreviatorConverter implements Converter<String, String> {

    private static final long serialVersionUID = 1L;
    
    private int maxWidth;

    public AbbreviatorConverter(int maxWidth) {
        this.maxWidth = maxWidth;
    }
    
    @Override
    public String convertToModel(String value, Class<? extends String> targetType, Locale locale)
            throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
        return value;
    }
    
    @Override
    public String convertToPresentation(String value, Class<? extends String> targetType, Locale locale)
            throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
        return StringUtils.abbreviate(value, maxWidth);
    }
    
    @Override
    public Class<String> getModelType() {
        return String.class;
    }
    
    @Override
    public Class<String> getPresentationType() {
        return String.class;
    }

}
