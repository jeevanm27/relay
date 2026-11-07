package io.relay.worker;

/** Thrown by a handler to say "retrying cannot help": the job goes straight to DEAD without using remaining attempts. */
public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
