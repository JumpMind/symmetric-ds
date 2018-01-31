package org.jumpmind.vaadin.ui.sqlexplorer;

import java.util.Arrays;
import java.util.Iterator;

import org.jumpmind.db.model.Trigger;
import org.jumpmind.vaadin.ui.common.CommonUiUtils;
import org.vaadin.aceeditor.AceEditor;
import org.vaadin.aceeditor.AceMode;

import com.vaadin.server.FontAwesome;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.MenuBar;
import com.vaadin.ui.MenuBar.Command;
import com.vaadin.ui.MenuBar.MenuItem;
import com.vaadin.ui.ProgressBar;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.TabSheet.SelectedTabChangeEvent;
import com.vaadin.ui.TabSheet.SelectedTabChangeListener;
import com.vaadin.ui.TabSheet.Tab;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.themes.ValoTheme;


public class TriggerInfoPanel extends VerticalLayout implements IInfoPanel {

	private static final long serialVersionUID = 1L;

	TabSheet tabSheet;

	String selectedCaption;
	
	boolean wrapSourceText;

	public TriggerInfoPanel(Trigger trigger, IDb db, Settings settings, String selectedTabCaption) {

		setSizeFull();

		tabSheet = CommonUiUtils.createTabSheet();
		tabSheet.addSelectedTabChangeListener(new SelectedTabChangeListener() {

			private static final long serialVersionUID = 1L;

			@Override
			public void selectedTabChange(SelectedTabChangeEvent event) {
				selectedCaption = tabSheet.getTab(tabSheet.getSelectedTab()).getCaption();
			}
		});
		addComponent(tabSheet);

		refreshSource(trigger);
		refreshDetails(trigger, db, settings);
							
		Iterator<Component> i = tabSheet.iterator();
        while (i.hasNext()) {
            Component component = i.next();
            Tab tab = tabSheet.getTab(component);
            if (tab.getCaption().equals(selectedTabCaption)) {
                tabSheet.setSelectedTab(component);
                break;
            }            
        }
	}
	
	public String getSelectedTabCaption() {
        return selectedCaption;
    }
	
	protected void refreshSource(final Trigger trigger) {
		VerticalLayout source = new VerticalLayout();
		source.setSizeFull();
		source.setSpacing(false);

		String sourceText = trigger.getSource();
		if (wrapSourceText) sourceText = wrapSource(sourceText);
		
		AceEditor editor = CommonUiUtils.createAceEditor();
		editor.setMode(AceMode.sql);
		editor.setValue(sourceText);
		editor.setSizeFull();
		source.addComponent(editor);
		source.setExpandRatio(editor, 1);
		
		HorizontalLayout bar = new HorizontalLayout();
		bar.setWidth(100, Unit.PERCENTAGE);
        bar.setMargin(new MarginInfo(false, true, false, true));
        
        MenuBar wrapSelect = new MenuBar();
        wrapSelect.addStyleName(ValoTheme.MENUBAR_BORDERLESS);
        wrapSelect.addStyleName(ValoTheme.MENUBAR_SMALL);
        MenuItem wrapButton = wrapSelect.addItem("Wrap text", new Command() {
        	private static final long serialVersionUID = 1L;
            @Override
            public void menuSelected(MenuItem selectedItem) {
                wrapSourceText = !wrapSourceText;
                tabSheet.removeTab(tabSheet.getTab(1));
                refreshSource(trigger);                
            }
        });
        wrapButton.setIcon(FontAwesome.ALIGN_JUSTIFY);
        
        bar.addComponent(wrapSelect);
        bar.setComponentAlignment(wrapSelect, Alignment.TOP_RIGHT);
        bar.setHeight((float)(2.5), Unit.REM);
        source.addComponent(bar);
		
		tabSheet.addTab(source, "Source");
		tabSheet.setSelectedTab(source);
	}
	
	private String wrapSource(String source) {
		String wrappedText = source;
		wrappedText = " "+wrappedText.replace('\n', ' ').trim()+" ";
		while (true) {
			String moreWrapped = wrappedText.replace("  ", " ");
			if (moreWrapped==wrappedText) {
				break;
			}
			wrappedText = moreWrapped;
		}
		
		int indentCount = 0, lastNewLineIndex = 0;
		boolean singleQuote = false, doubleQuote = false;
		String[] incIndentBeforeTerms = {},
				incIndentAfterTerms = {"begin", "case when", "delete from", "drop", "else", "if",
						"select"},
				decIndentBeforeTerms = {"end if", "end", "else"},
				decIndentAfterTerms = {},
				noIndentTerms = {"after", "before", "call", "create", "declare", "for each row",
						"instead of", "return", "start", "then", "when", "where"},
				noNewLineAfterTerms = {"after", "before", "call", "create", "declare", "delete from",
						"drop", "if", "instead of", "select", "start"},
				noNewLineBeforeTerms = {"then"},
				allTerms = concatAll(incIndentBeforeTerms, incIndentAfterTerms, decIndentBeforeTerms, 
						decIndentAfterTerms, noIndentTerms);
		
		indexLoop: for (int i=0; i<wrappedText.length()-1; i++) {
			if (wrappedText.charAt(i) == '\"') {
				if (singleQuote || (doubleQuote && wrappedText.charAt(i-1) != '\\')) {
					doubleQuote = false;
				} else {
					doubleQuote = true;
				}
			} else if (wrappedText.charAt(i) == '\'') {
				if (doubleQuote || (singleQuote && wrappedText.charAt(i-1) != '\\')) {
					singleQuote = false;
				} else {
					singleQuote = true;
				}
			}
			boolean isInQuotes = singleQuote || doubleQuote;
			
			if (!isInQuotes && wrappedText.charAt(i) == ';') {
				wrappedText = insertNewLine(wrappedText, i, indentCount);
				lastNewLineIndex = i;
				i += 4*indentCount;
			} else if (!isInQuotes && wrappedText.charAt(i) == '(') {
				indentCount += 2;
			} else if (!isInQuotes && wrappedText.charAt(i) == ')') {
				indentCount -= 2;
			}
			
			if (i-lastNewLineIndex > 100) {
				if (isInQuotes) {
					int lookBack = 0;
					while (lookBack < i &&
							wrappedText.charAt(i-lookBack) != '\"' &&
							wrappedText.charAt(i-lookBack) != '\'') {
						lookBack++;
					}
					if (i-lookBack <= lastNewLineIndex+1) {
						continue indexLoop;
					} else {
						wrappedText = insertNewLine(wrappedText, i-lookBack, indentCount);
						lastNewLineIndex = i-lookBack;
						i += 4*indentCount+1;
						continue indexLoop;
					}
				} else {
					int lookBack = 0;
					while (lookBack < i &&
							wrappedText.charAt(i-lookBack) != ',' &&
							wrappedText.charAt(i-lookBack) != '.' &&
							wrappedText.charAt(i-lookBack) != ' ' &&
							wrappedText.charAt(i-lookBack) != '\"' &&
							wrappedText.charAt(i-lookBack) != '\'') {
						lookBack++;
					}
					if (i-lookBack <= lastNewLineIndex+1) {
						continue indexLoop;
					} else {
						wrappedText = insertNewLine(wrappedText, i-lookBack+1, indentCount);
						lastNewLineIndex = i-lookBack+1;
						i += 4*indentCount+1;
						continue indexLoop;
					}
				}
			}
			
			for (String term : allTerms) {
				if (i+term.length() < wrappedText.length() && !isInQuotes &&
						termAppearsIn(wrappedText, term, i)) {
					if (Arrays.asList(incIndentBeforeTerms).contains(term)) {
						indentCount++;
					} else if (Arrays.asList(decIndentBeforeTerms).contains(term)) {
						indentCount--;
					}
					
					if (!Arrays.asList(noNewLineBeforeTerms).contains(term)) {
						wrappedText = insertNewLine(wrappedText, i, indentCount);
						lastNewLineIndex = i;
						i += 4*indentCount;
					}
					i += term.length();
					
					if (Arrays.asList(incIndentAfterTerms).contains(term)) {
						indentCount++;
					} else if (Arrays.asList(decIndentAfterTerms).contains(term)) {
						indentCount--;
					}
					if (!Arrays.asList(noNewLineAfterTerms).contains(term)) {
						wrappedText = insertNewLine(wrappedText, i+1, indentCount);
						lastNewLineIndex = i;
						i += 4*indentCount;
					}
					
					i--;
					continue indexLoop;
				}
			}
			
		}
		
		return wrappedText.trim();
	}
	
	private String[] concatAll(String[] ... arrays) {
		int size = 0, index =0;
		for (String[] array : arrays) {
			size += array.length;
		}
		String [] all = new String[size];
		for (String[] array : arrays) {
			System.arraycopy(array, 0, all, index, array.length);
			index += array.length;
		}
		return all;
	}
	
	private boolean termAppearsIn(String str, String term, int index) {
		return str.regionMatches(true, index, term, 0, term.length()) &&
			(index==0 || !(
				Character.isLetter(str.charAt(index-1)) ||
				str.charAt(index-1) == '@' ||
				str.charAt(index-1) == '$' ||
				str.charAt(index-1) == '#' ||
				str.charAt(index-1) == '_')) &&
			(index+term.length() >= str.length() || !(
				Character.isLetter(str.charAt(index+term.length())) ||
				str.charAt(index+term.length()) == '@' ||
				str.charAt(index+term.length()) == '$' ||
				str.charAt(index+term.length()) == '#' ||
				str.charAt(index+term.length()) == '_'));
	}
	
	private String insertNewLine(String str, int index, int indentCount) {
		if (str.charAt(index) == ';') index++;
		String text = str.substring(0, index) + "\n";
		for (int j=0; j<indentCount; j++) {
			text += "    ";
		}
		if (str.charAt(index) == ' ') index++;
		text += str.substring(index);
		return text;
	}
	
	protected void refreshDetails(final Trigger trigger, final IDb db, final Settings settings) {
		
		final HorizontalLayout executingLayout = new HorizontalLayout();
        executingLayout.setSizeFull();
        final ProgressBar p = new ProgressBar();
        p.setIndeterminate(true);
        executingLayout.addComponent(p);
        tabSheet.addTab(executingLayout, "Details", FontAwesome.SPINNER, 0);
        tabSheet.setSelectedTab(executingLayout);
        
        TriggerTableLayout triggerTable = new TriggerTableLayout(trigger, settings, new Refresher() {
        	public void refresh() {
        		tabSheet.removeTab(tabSheet.getTab(0));
        		refreshDetails(trigger, db, settings);
        	}
        });
        
        boolean select = tabSheet.getSelectedTab().equals(executingLayout);
        tabSheet.removeComponent(executingLayout);
        VerticalLayout layout = new VerticalLayout();
        layout.setMargin(true);
        layout.setSizeFull();
        layout.addComponent(triggerTable);
        tabSheet.addTab(layout, "Details", null, 0);
        if (select) {
            tabSheet.setSelectedTab(layout);
        }
	}
	
	public class Refresher {
    	public void refresh(){
    	};
    }

	@Override
	public void selected() {
	}

	@Override
	public void unselected() {
	}

}
