package org.jumpmind.symmetric.admin;

public class BlankScreen extends AbstractScreen {

    private static final long serialVersionUID = 1370281891130293550L;

    @Override
    public ScreenName getScreenName() {
        return  ScreenName.BLANK;
    }

    @Override
    public void setup(SymmetricDatabase c) {
    }

}
