import com.google.common.collect.Lists;
import com.zjw.source.TailSubDirectorySource;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zjw on 2017/6/19.
 */
public class TestSource {

    private TailSubDirectorySource source;
    private Context context;
    private Channel channel;
    private ChannelSelector rcs = new ReplicatingChannelSelector();

    @Before
    public void before() {
        this.context = new Context();
        this.context.put("file", TestConstants.FILE);
        this.context.put("positionDir", TestConstants.POSITION_DIR);
        this.context.put("spoolDir", TestConstants.SPOOL_DIRECTORY);

        source = new TailSubDirectorySource();
        channel = new MemoryChannel();
        rcs.setChannels(Lists.newArrayList(channel));
        source.setChannelProcessor(new ChannelProcessor(rcs));
    }

    @Test
    public void test() {
        source.configure(context);
        source.start();
    }

}
