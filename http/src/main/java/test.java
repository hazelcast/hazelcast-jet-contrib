import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.contrib.http.HttpSinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.test.TestSources;

public class test {
    public static void main(String[] args) {

        JetInstance jet = Jet.newJetInstance();

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(5))
                .withoutTimestamps()
                .writeTo(HttpSinks.sse("/items", 100));

        Job job = jet.newJob(p);
        String sseAddress = HttpSinks.getSseAddress(jet, job);
        System.out.println("sseAddress = " + sseAddress);


    }

}
