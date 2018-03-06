package io.smls.predictor.message;

import java.io.Serializable;

/**
 * Interface of Akka message.
 */
public interface AkkaMessage extends Serializable {
    static final long serialVersionUID = 1L;
    public boolean isRequest();
}
