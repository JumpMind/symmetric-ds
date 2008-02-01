package org.jumpmind.symmetric.admin;

import javax.swing.JPanel;

abstract public class AbstractScreen extends JPanel {

    abstract public void setup(SymmetricDatabase c);
    
    abstract public ScreenName getScreenName();
}
