package com.datasujh.ras.hbase;

import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.converters.IntegerConverter;
import org.apache.commons.beanutils.converters.LongConverter;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.avro.mapred.AvroAsTextInputFormat;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by rsingh on 3/24/14.
 */
public class NJASKImporter {

    private HBaseAdmin hadmin;
    private String hbaseHost;
    private String hbasePort;
    private Configuration hConf;
    private boolean hbaseIsConfigured;


    private static final String NJASK_TABLE = "jerseycity_njask";
    private static final String HBASE_HOST = "hadoop2.jcboe.org";
    private static final String HBASE_PORT = "2181";

    public static class NJASKImporterMapper implements org.apache.hadoop.mapred.Mapper<Text, Text, ImmutableBytesWritable, Writable> {

        private ImmutableBytesWritable ibw = new ImmutableBytesWritable();
        private final String COLUMN_FAMILY = "fields";
        private final String UNDERSCORE = "_";
        JSONParser parser = new JSONParser();
        Map fieldMap = null;
        String year = null;

        public void configure(JobConf job) {
            ConvertUtils.register(new IntegerConverter(null), Integer.class);
            ConvertUtils.register(new LongConverter(null), Long.class);
            year = job.get("year");
        }

        public void close() throws IOException {// TODO Auto-generated method stub
        }

        public void map(Text key, Text value, OutputCollector<ImmutableBytesWritable, Writable> collector, Reporter reporter) throws IOException {


            try {
                fieldMap = (Map) parser.parse(key.toString());
                Map njAskMap = new HashMap();

                Long localStudentID = fieldMap.get("LocalStudentID")==null?0:NumberUtils.toLong(fieldMap.get("LocalStudentID").toString());
                Long stateID = fieldMap.get("SID")==null?0:NumberUtils.toLong(fieldMap.get("SID").toString());
                
                Put put = new Put(Bytes.toBytes(localStudentID+ UNDERSCORE+stateID+ UNDERSCORE + year));

//                Long lalScaleScore = (Long)fieldMap.get("LALScaleScore")==null?0:(Long)fieldMap.get("LALScaleScore");
//                Long mathScaleScore = fieldMap.get("MathScaleScore")==null?0:(Long)fieldMap.get("MathScaleScore");

                Iterator it = fieldMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pairs = (Map.Entry)it.next();
                    if(pairs.getValue()!=null){
                        put.add(Bytes.toBytes(COLUMN_FAMILY),
                                Bytes.toBytes(pairs.getKey().toString()), Bytes.toBytes(pairs.getValue().toString()));
                    }
                    it.remove(); // avoids a ConcurrentModificationException
                }

//                put.add(Bytes.toBytes(COLUMN_FAMILY),
//                        Bytes.toBytes("LALScaleScore"), Bytes.toBytes(lalScaleScore));
//                put.add(Bytes.toBytes(COLUMN_FAMILY),
//                        Bytes.toBytes("MathScaleScore"), Bytes.toBytes(mathScaleScore));
                collector.collect(ibw, put);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {


        try {
            CommandLine cmdLine = parseArgs(args);
            NJASKImporter njaskImporter = new NJASKImporter();
            //Run Init only if create is set, this option will re-create the HBase tables.
            if (cmdLine.hasOption("create")) {
                njaskImporter.init();
            }
            Configuration hConf = HBaseConfiguration.create();
            hConf.set("hbase.zookeeper.quorum", HBASE_HOST);
            hConf.set("hbase.zookeeper.property.clientPort", HBASE_PORT);
            hConf.set("year", cmdLine.getOptionValue("year"));

            JobConf njaskLoad = new JobConf(hConf);
            njaskLoad.setJobName("NJAskLoader");
            njaskLoad.setJarByClass(NJASKImporterMapper.class);
            njaskLoad.setMapperClass(NJASKImporterMapper.class);
            njaskLoad.setInputFormat(AvroAsTextInputFormat.class);
            njaskLoad.setOutputFormat(TableOutputFormat.class);
            njaskLoad.set(TableOutputFormat.OUTPUT_TABLE, NJASK_TABLE);
            njaskLoad.setOutputKeyClass(ImmutableBytesWritable.class);
            njaskLoad.setOutputValueClass(Writable.class);
            njaskLoad.setNumReduceTasks(0);
            FileInputFormat.addInputPath(njaskLoad, new Path(cmdLine.getOptionValue("input")));
            RunningJob runningJob = JobClient.runJob(njaskLoad);

        } catch (Exception e) {
            e.printStackTrace(System.out);
            System.exit(1);
        }
    }


    private static CommandLine parseArgs(String[] args) throws org.apache.commons.cli.ParseException {

        Options options = new Options();

        Option o;

        o = new Option("i", "input", true, "HDFS dir defining input.");
        o.setArgName("input");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("y", "year", true, "NjAsk Year");
        o.setArgName("year");
        o.setRequired(false);
        options.addOption(o);

        o = new Option("c", "create", true, "Re-Create the HBase Table");
        o.setArgName("create");
        o.setRequired(false);
        options.addOption(o);

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
            if (!cmd.hasOption("input")) {
                throw new RuntimeException("Invalid usage");
            }

        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();

            formatter.printHelp("NjAskLoader ", options, true);
            System.exit(1);
        }

        return cmd;
    }

    public void init() throws Exception {
        /** Auto-vivify htables */
        doTableRecreation();
    }

    private void configureHBase() throws Exception {
        if (hbaseIsConfigured) {
            return;
        }

        hConf = HBaseConfiguration.create();
        hConf.set("hbase.zookeeper.quorum", HBASE_HOST);
        hConf.set("hbase.zookeeper.property.clientPort", HBASE_PORT);
        hadmin = new HBaseAdmin(hConf);
        hbaseIsConfigured = true;
    }

    public void doTableRecreation() throws Exception {
        configureHBase();

        /** Drop and recreate tables if we're in test mode */
        if (hadmin.tableExists(NJASK_TABLE)) {
            System.out.println(
                    "[[[WARNING]]] NJAskLoader: doTableRecreation is on, so we're dropping and recreating our htable:"
                            + NJASK_TABLE);
            hadmin.disableTable(Bytes.toBytes(NJASK_TABLE));
            hadmin.deleteTable(Bytes.toBytes(NJASK_TABLE));
        }

        doTableCreation();
    }

    private void doTableCreation() throws Exception {
        configureHBase();

        /** Auto-vivify tables if they don't exist */
        if (!hadmin.tableExists(NJASK_TABLE)) {
            HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(NJASK_TABLE));

            desc.addFamily(new HColumnDescriptor(Bytes.toBytes("fields")));
            hadmin.createTable(desc);
        }
    }


}
