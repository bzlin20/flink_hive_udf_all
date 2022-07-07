package com.ds.hive.udaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TruckElectricDistanceUdaf extends AbstractGenericUDAFResolver {
    private static final Logger logger = LoggerFactory.getLogger(TruckElectricDistanceUdaf.class);

    public TruckElectricDistanceUdaf() {
        logger.info("get start!!");
    }

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 5)
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly five argument is expected." + parameters.length);
        return new GenericUDAFMkListEvaluator();
    }

    public static class GenericUDAFMkListEvaluator extends GenericUDAFEvaluator {
        private StandardMapObjectInspector mapOI;

        private Map<String, Double> preXMap = new HashMap<>();

        private Map<String, Double> preYMap = new HashMap<>();

        private Map<String, Double[]> distanceRecordMap = (Map)new HashMap<>();

        private Map<String, Integer> preElectricMap = new HashMap<>();

        private Map<String, Integer> lockElectricMap = new HashMap<>();

        private Map<String, Boolean> recordTagMap = new HashMap<>();

        private Map<String, Boolean> insertTagMap = new HashMap<>();

        private List<String> discussTruck = new ArrayList<>();

        public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == GenericUDAFEvaluator.Mode.PARTIAL1)
                return (ObjectInspector)ObjectInspectorFactory.getStandardMapObjectInspector((ObjectInspector)PrimitiveObjectInspectorFactory.javaStringObjectInspector,

                        (ObjectInspector)ObjectInspectorFactory.getStandardListObjectInspector((ObjectInspector)ObjectInspectorFactory.getStandardListObjectInspector((ObjectInspector)PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)));
            if (m == GenericUDAFEvaluator.Mode.PARTIAL2) {
                this.mapOI = (StandardMapObjectInspector)parameters[0];
                return (ObjectInspector)ObjectInspectorFactory.getStandardMapObjectInspector((ObjectInspector)PrimitiveObjectInspectorFactory.javaStringObjectInspector,

                        (ObjectInspector)ObjectInspectorFactory.getStandardListObjectInspector((ObjectInspector)ObjectInspectorFactory.getStandardListObjectInspector((ObjectInspector)PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)));
            }
            if (m == GenericUDAFEvaluator.Mode.FINAL) {
                this.mapOI = (StandardMapObjectInspector)parameters[0];
                return (ObjectInspector)PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }
            if (m == GenericUDAFEvaluator.Mode.COMPLETE)
                return (ObjectInspector)PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            throw new RuntimeException("no such mode Exception");
        }

        static class ElectricDistanceAggregationBuffer implements GenericUDAFEvaluator.AggregationBuffer {
            Map<String, ArrayList<ArrayList<Double>>> container;
        }

        public void reset(GenericUDAFEvaluator.AggregationBuffer agg) throws HiveException {
            ((ElectricDistanceAggregationBuffer)agg).container = new HashMap<>();
        }

        public GenericUDAFEvaluator.AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ElectricDistanceAggregationBuffer ret = new ElectricDistanceAggregationBuffer();
            reset(ret);
            return ret;
        }

        public void iterate(GenericUDAFEvaluator.AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert parameters.length == 5;
            String p = String.valueOf(parameters[0]);
            double x = ((Double)parameters[1]).doubleValue();
            double y = ((Double)parameters[2]).doubleValue();
            int w = ((Integer)parameters[3]).intValue();
            String t = (String)parameters[4];
            TruckElectricDistanceUdaf.logger.info(t + "->" + p + "from:(" + this.preXMap + "," + this.preYMap + ")to:(" + x + "," + y + ")");
            if (p != null && x != 0.0D && y != 0.0D)
                if (!this.discussTruck.contains(t)) {
                    this.preXMap.put(t, Double.valueOf(0.0D));
                    this.preYMap.put(t, Double.valueOf(0.0D));
                    this.distanceRecordMap.put(t, new Double[] { Double.valueOf(0.0D), Double.valueOf(0.0D) });
                    this.preElectricMap.put(t, Integer.valueOf(-1));
                    this.lockElectricMap.put(t, Integer.valueOf(-1));
                    this.recordTagMap.put(t, Boolean.valueOf(false));
                    this.insertTagMap.put(t, Boolean.valueOf(false));
                    this.discussTruck.add(t);
                } else {
                    if (Integer.valueOf(p).intValue() + 1 != ((Integer)this.preElectricMap.get(t)).intValue() &&
                            Integer.valueOf(p).intValue() != ((Integer)this.preElectricMap.get(t)).intValue()) {
                        this.lockElectricMap.put(t, Integer.valueOf(Integer.valueOf(p).intValue()));
                        this.distanceRecordMap.put(t, new Double[] { Double.valueOf(0.0D), Double.valueOf(0.0D) });
                    }
                    if (Integer.valueOf(p).intValue() == ((Integer)this.lockElectricMap.get(t)).intValue()) {
                        this.recordTagMap.put(t, Boolean.valueOf(false));
                        this.insertTagMap.put(t, Boolean.valueOf(false));
                    } else {
                        if (Integer.valueOf(p).intValue() + 1 == ((Integer)this.preElectricMap.get(t)).intValue() && ((Boolean)this.recordTagMap
                                .get(t)).booleanValue() == true) {
                            this.insertTagMap.put(t, Boolean.valueOf(true));
                        } else {
                            this.insertTagMap.put(t, Boolean.valueOf(false));
                        }
                        if (Math.abs(((Double)this.preXMap.get(t)).doubleValue() - x) < 100.0D && Math.abs(((Double)this.preYMap.get(t)).doubleValue() - y) < 100.0D) {
                            this.recordTagMap.put(t, Boolean.valueOf(true));
                        } else {
                            this.recordTagMap.put(t, Boolean.valueOf(false));
                        }
                    }
                    if (((Boolean)this.insertTagMap.get(t)).booleanValue()) {
                        ElectricDistanceAggregationBuffer myagg = (ElectricDistanceAggregationBuffer)agg;
                        insertMap(String.valueOf(this.preElectricMap.get(t)), myagg, this.distanceRecordMap.get(t));
                        this.distanceRecordMap.put(t, new Double[] { Double.valueOf(0.0D), Double.valueOf(0.0D) });
                    }
                    if (((Boolean)this.recordTagMap.get(t)).booleanValue()) {
                        Double[] distanceRecordTemp = this.distanceRecordMap.get(t);
                        if (w < 60) {
                            Double[] arrayOfDouble = distanceRecordTemp;
                            arrayOfDouble[0] = Double.valueOf(arrayOfDouble[0].doubleValue() + Math.sqrt(Math.pow(((Double)this.preXMap.get(t)).doubleValue() - x, 2.0D) + Math.pow(((Double)this.preYMap.get(t)).doubleValue() - y, 2.0D)));
                            Double.valueOf(arrayOfDouble[0].doubleValue() + Math.sqrt(Math.pow(((Double)this.preXMap.get(t)).doubleValue() - x, 2.0D) + Math.pow(((Double)this.preYMap.get(t)).doubleValue() - y, 2.0D)));
                            this.distanceRecordMap.put(t, distanceRecordTemp);
                        } else {
                            Double[] arrayOfDouble = distanceRecordTemp;
                            arrayOfDouble[1] = Double.valueOf(arrayOfDouble[1].doubleValue() + Math.sqrt(Math.pow(((Double)this.preXMap.get(t)).doubleValue() - x, 2.0D) + Math.pow(((Double)this.preYMap.get(t)).doubleValue() - y, 2.0D)));
                            Double.valueOf(arrayOfDouble[1].doubleValue() + Math.sqrt(Math.pow(((Double)this.preXMap.get(t)).doubleValue() - x, 2.0D) + Math.pow(((Double)this.preYMap.get(t)).doubleValue() - y, 2.0D)));
                            this.distanceRecordMap.put(t, distanceRecordTemp);
                        }
                    }
                    this.preElectricMap.put(t, Integer.valueOf(Integer.valueOf(p).intValue()));
                    this.preXMap.put(t, Double.valueOf(x));
                    this.preYMap.put(t, Double.valueOf(y));
                }
        }

        public Object terminatePartial(GenericUDAFEvaluator.AggregationBuffer agg) throws HiveException {
            ElectricDistanceAggregationBuffer myagg = (ElectricDistanceAggregationBuffer)agg;
            Map<String, ArrayList<ArrayList<Double>>> ret = new HashMap<>(myagg.container);
            for (String key : ret.keySet())
                TruckElectricDistanceUdaf.logger.info("terminatePartial contains:" + key);
            return ret;
        }

        public void merge(GenericUDAFEvaluator.AggregationBuffer agg, Object partial) throws HiveException {
            ElectricDistanceAggregationBuffer myagg = (ElectricDistanceAggregationBuffer)agg;
            Map partialResult = this.mapOI.getMap(partial);
            for (Object key : partialResult.keySet()) {
                TruckElectricDistanceUdaf.logger.info("merge contains:" + key + partialResult.get(key).toString());
                ArrayList<Object> bundleList = (ArrayList<Object>)partialResult.get(key);
                for (int i = 0; i < bundleList.size(); i++) {
                    ArrayList<Double> singleList = (ArrayList<Double>)bundleList.get(i);
                    Double[] addLength = new Double[singleList.size()];
                    for (int j = 0; j < singleList.size(); j++)
                        addLength[j] = singleList.get(j);
                    insertMap(key.toString(), myagg, addLength);
                }
            }
        }

        public String terminate(GenericUDAFEvaluator.AggregationBuffer agg) throws HiveException {
            Map<Object, Double> map = new HashMap<>();
            ElectricDistanceAggregationBuffer myagg = (ElectricDistanceAggregationBuffer)agg;
            StringBuilder resultString = new StringBuilder();
            // 代码有问题，先注释
//            for (Map.Entry<String, ArrayList<ArrayList<Double>>> entry : myagg.container.entrySet()) {
//                String key = entry.getKey();
////                ArrayList<Double> bundleList = (ArrayList<Double>)entry.getValue();
//
//                resultString.append("/").append(key).append(":");
//                for (int i = 0; i < bundleList.size(); i++) {
//                    ArrayList<Double> singleList = (ArrayList<Double>)bundleList.get(i);
//                    double totalDistance = 0.0D;
//                    for (int j = 0; j < singleList.size(); j++)
//                        totalDistance += ((Double)singleList.get(j)).doubleValue();
//                    resultString.append(totalDistance).append(";");
//                    TruckElectricDistanceUdaf.logger.info("terminate contains:" + key);
//                }
//            }
            return resultString.toString();
        }

        private void insertMap(String p, ElectricDistanceAggregationBuffer myagg, Double[] recordLength) {
            if (recordLength[0].doubleValue() != 0.0D || recordLength[1].doubleValue() != 0.0D)
                if (!myagg.container.containsKey(p)) {
                    ArrayList<ArrayList<Double>> insertList = new ArrayList<>();
                    ArrayList<Double> insertLength = new ArrayList<>();
                    insertLength.add(recordLength[0]);
                    insertLength.add(recordLength[1]);
                    insertList.add(insertLength);
                    myagg.container.put(p, insertList);
                } else {
                    ArrayList<ArrayList<Double>> insertList = myagg.container.get(p);
                    ArrayList<Double> insertLength = new ArrayList<>();
                    insertLength.add(recordLength[0]);
                    insertLength.add(recordLength[1]);
                    insertList.add(insertLength);
                    myagg.container.put(p, insertList);
                }
        }
    }
}
