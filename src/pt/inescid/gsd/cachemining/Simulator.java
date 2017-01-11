package pt.inescid.gsd.cachemining;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Result;

public class Simulator {

    private static Cache<Result> cache = new Cache<Result>();

    private static SequenceEngine sequenceEngine = new SequenceEngine();

    HTable htable;

    public Simulator() {
        // htable = new HTable(conf, tableName);
    }

    private void findSequences(String getsFilename) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(getsFilename));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith("###")) {
                    continue;
                }

                String[] elements = line.split(":");
                String tableName = elements[1];
                String colFamily = elements[3];
                String colQualifier = elements.length == 5 ? elements[4] : "";

                String key = tableName + SequenceEngine.SEPARATOR + colFamily + SequenceEngine.SEPARATOR + colQualifier;

                System.out.println("KEY ---> " + key);

                List<String> sequence = sequenceEngine.getSequence(key);
                if (sequence == null) {
                    System.out.println("NO SEQUENCE.");
                    continue;
                }
                System.out.print("SEQUENCE-> ");
                for (String container : sequence) {
                    elements = container.split(":");
                    String containerTableName = elements[0];
                    String containerColFamily = elements[1];
                    // String containerColQualifier = elements[2];

                    System.out.print(container + ", ");
                }
                System.out.println();

            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Args error!");
            return;
        }
        Simulator simulator = new Simulator();
        simulator.findSequences(args[0]);

    }
}
