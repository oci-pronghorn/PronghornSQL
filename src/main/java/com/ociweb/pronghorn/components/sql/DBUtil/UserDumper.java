package com.ociweb.pronghorn.components.sql.DBUtil;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import static com.ociweb.pronghorn.ring.RingReader.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;

public class UserDumper extends PronghornStage {
    
    public interface Decoder {
        public abstract boolean decode(RingBuffer ring, int templateID, List<Object> output) throws Exception;
    }

    private RingBuffer ring;
    private Decoder decoder;
    private List<Object> output;
    private boolean loop = true;
    private org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserDumper.class);
    
    public UserDumper(GraphManager gm, RingBuffer ring, Decoder decoder) {
        super(gm,ring, NONE);
        this.ring = ring;
        this.decoder = decoder;
        this.output = new ArrayList<Object>();
    }

    @Override
    public void run() {
        try {
            FieldReferenceOffsetManager FROM = RingBuffer.from(ring);

            do {
                if (tryReadFragment(ring)) {
                    if (isNewMessage(ring)) {
                        int msgLoc = getMsgIdx(ring);
                        int templateID = (int)FROM.fieldIdScript[msgLoc];
                        if (!decoder.decode(ring, templateID, output)) {
                            logger.error("Dumper: Unknown template ID " + templateID);
                            throw new NotImplementedException("Dumper: Unknown template ID " + templateID);
                        }
                    } // if isNewMessage
                } else { // while tryReadFragment
                    // do something else
                    Thread.yield();
                }
            } while (loop || (RingBuffer.contentRemaining(ring) > 0));
        } catch (Exception e) {
            logger.error("Dumper: " + e.getClass().getName() + ": " + e.getMessage(), e);
        }
    }

    public void stop() {
        loop = false;
    }

    public List<Object> result() {
        return output;
    }
}