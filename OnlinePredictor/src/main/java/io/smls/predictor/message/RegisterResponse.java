package io.smls.predictor.message;

/**
 * Response of register.
 */
public class RegisterResponse implements AkkaMessage {
    private static final long serialVersionUID = -6105774044776758248L;
    private final int mStatus;
    private final String mExtraInfo;

    public RegisterResponse(int status, String extraInfo){
        mStatus = status;
        mExtraInfo = extraInfo;
    }

    @Override
    public boolean isRequest() {
        return false;
    }

    public int getStatus() {
        return mStatus;
    }

    public String getExtraInfo() {
        return mExtraInfo;
    }

}
