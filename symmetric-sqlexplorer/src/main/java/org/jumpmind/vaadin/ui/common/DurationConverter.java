package org.jumpmind.vaadin.ui.common;

import java.util.Locale;

import com.vaadin.v7.data.util.converter.StringToLongConverter;

public class DurationConverter extends StringToLongConverter {

    private static final long serialVersionUID = 1L;

    public DurationConverter() {
    }

    @Override
    public String convertToPresentation(Long value, Class<? extends String> targetType, Locale locale)
            throws com.vaadin.v7.data.util.converter.Converter.ConversionException {
        return value != null ? CommonUiUtils.formatDuration(value) : "";
    }
}
