package io.smls.predictor.message;

/**
 * Request of predict.
 */
public final class PredictRequest implements AkkaMessage {
    private static final long serialVersionUID = 5664612200359134497L;
    private final long mRequestId;
    private final String mJsonData;

    public PredictRequest(long id, String jsonData){
        mRequestId = id;
        mJsonData = jsonData;
    }

    public long getRequestId() {
        return mRequestId;
    }

    public String getData() {
        return mJsonData;
    }

    @Override
    public boolean isRequest() {
        return true;
    }
}
