import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

public class ChunkedReadStream  implements ReadStream<Buffer> {

    private final int chunkSize;
    private final Buffer buffer;
    private final int length;
    private final Context context = Vertx.currentContext();
    private boolean paused;
    private int sent;
    private Handler<Buffer> dataHandler;
    private Handler<Void> endHandler;

    public ChunkedReadStream(final Buffer buffer, final int chunkSize) {
        this.chunkSize = chunkSize;
        this.buffer = buffer;
        this.length = buffer.length();
    }

    @Override
    public ReadStream<Buffer> handler(final Handler<Buffer> handler) {
        dataHandler = handler;
        return this;
    }

    public void send() {
        if (!paused) {
            if (sent < length) {
                int start = sent;
                int end = sent + chunkSize;
                if (end > length) {
                    end = length;
                }
                sent = end;
                dataHandler.handle(buffer.slice(start, end));
                context.runOnContext(v -> {
                    send();
                });
            } else {
                if (endHandler != null) {
                    endHandler.handle(null);
                }
            }
        }
    }

    @Override
    public ReadStream<Buffer> pause() {
        paused = true;
        return this;
    }

    @Override
    public ReadStream<Buffer> resume() {
        paused = false;
        send();
        return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(final Handler<Void> handler) {
        endHandler = handler;
        return this;
    }

    @Override
    public ReadStream<Buffer> exceptionHandler(final Handler<Throwable> handler) {
        return this;
    }
}