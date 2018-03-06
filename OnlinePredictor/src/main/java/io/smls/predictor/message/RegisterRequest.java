package io.smls.predictor.message;

/**
 * Request of register.
 */
public class RegisterRequest implements AkkaMessage {
    private static final long serialVersionUID = -964632617185332684L;
    private final String mPath;

    public RegisterRequest(String path){
        mPath = path;
    }

    @Override
    public boolean isRequest() {
        return true;
    }

    public String getPath() {
        return mPath;
    }
}
