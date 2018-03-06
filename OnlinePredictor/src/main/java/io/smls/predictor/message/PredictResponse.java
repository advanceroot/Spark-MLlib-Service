package io.smls.predictor.message;

/**
 * Response of predict.
 */
public class PredictResponse implements AkkaMessage {
    private static final long serialVersionUID = 4752651338127001570L;
    private final long mRequestId;
    private final String mResult;

    public PredictResponse(long id, String result){
        mRequestId = id;
        mResult = result;
    }

    public long getRequestId() {
        return mRequestId;
    }

    public String getResult() {
        return mResult;
    }

    @Override
    public boolean isRequest() {
        return false;
    }
}
