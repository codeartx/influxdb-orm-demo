package com.example.influxdbormdemo.util;

import com.example.influxdbormdemo.anno.MeasurementPrefix;
import com.example.influxdbormdemo.anno.MeasurementSuffix;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.util.CollectionUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class InfluxDBUtil {
    public static String measurementName(Object o) {
        Class clazz = o.getClass();

        // 获取 @Measurement
        Measurement meaAnno = (Measurement) clazz.getAnnotation(Measurement.class);
        if (null == meaAnno) {
            return null;
        }

        String measurement = meaAnno.name();

        // 获取 @MeasurementPrefix 字段的值
        String prefix = getFieldValueByAnno(o, MeasurementPrefix.class);

        // 获取 @MeasurementSuffix 字段的值
        String suffix = getFieldValueByAnno(o, MeasurementSuffix.class);

        if (null != prefix) {
            measurement = prefix + "_" + measurement;
        }

        if (null != suffix) {
            measurement = measurement + "_" + suffix;
        }

        return measurement;
    }

    @Nullable
    private static String getFieldValueByAnno(Object o, Class annoClazz) {
        Class clazz = o.getClass();
        String val = null;

        for (Field field : clazz.getDeclaredFields()) {
            Annotation anno = field.getAnnotation(annoClazz);
            if (null != anno) {
                try {
                    field.setAccessible(true);
                    val = (String) field.get(o);
                } catch (IllegalAccessException e) {
                    log.error("failed to get value of field: {}", field.getName(), e);
                }
            }
        }

        return val;
    }

    /**
     * 将 @Measurement 注解的对象转换为 Influxdb Point 对象
     */
    public static Point measurement2Point(Object o) {
        String measurement = measurementName(o);
        if (null == measurement) {
            return null;
        }

        Class clazz = o.getClass();

        // 遍历所有的 @Column 字段
        Point point = Point.measurement(measurement);
        for (Field field : clazz.getDeclaredFields()) {
            Column colAnno = field.getAnnotation(Column.class);
            MeasurementPrefix prefixAnno = field.getAnnotation(MeasurementPrefix.class);
            MeasurementSuffix suffixAnno = field.getAnnotation(MeasurementSuffix.class);
            if (null == colAnno || null != suffixAnno || null != prefixAnno) {
                continue;
            }

            try {
                field.setAccessible(true);

                String name = colAnno.name();
                boolean tag = colAnno.tag();
                if (tag) {
                    point.addTag(field.getName(), (String) field.get(o));
                } else {
                    if (Objects.equals("time", name)) {
                        point.time((Instant) field.get(o), WritePrecision.MS);
                    } else {
                        point.addField(field.getName(), (long) field.get(o));
                    }
                }
            } catch (IllegalAccessException e) {
                log.error("failed to get value of field: {}", field.getName(), e);
            }
        }

        return point;
    }

    /**
     * 写入
     */
    public static void write(InfluxDBClient influxDB, Object measurement) {
        WriteApiBlocking w = influxDB.getWriteApiBlocking();
        Point point = measurement2Point(measurement);
        if (null == point) {
            return;
        }

        w.writePoint(point);
    }

    /**
     * 查询某个 measurement 的某个字段
     */
    public static <T> List<T> query1d(InfluxDBClient influxDB,
                                      String bucket,
                                      String measurement, Class<T> clazz) {
        return query1d(influxDB, bucket, measurement, null, clazz);
    }

    public static <T> List<T> query1d(InfluxDBClient influxDB,
                                      String bucket,
                                      String measurement, List<String> fields, Class<T> clazz) {
        String query = baseQ(bucket, measurement, fields, "-1d");
        return doQuery(influxDB, clazz, query);
    }

    private static <T> List<T> doQuery(InfluxDBClient influxDB, Class<T> clazz, String query) {
        List<FluxTable> ts = influxDB.getQueryApi().query(query);
        List<T> res = InfluxDBUtil.fluxTable2Pojo(ts, clazz);
        return res;
    }

    @NotNull
    private static String baseQ(String bucket, String measurement, List<String> fields, String start) {
        String query = "from(bucket: \"" + bucket + "\")" +
                " |> range(start: " + start + ")" +
                " |> filter(fn: (r) => r._measurement == \"" + measurement + "\")";

        if (!CollectionUtils.isEmpty(fields)) {
            query += " |> filter(fn: (r) => ";

            for (int i = 0; i < fields.size(); i++) {
                String field = fields.get(i);
                if (i == 0) {
                    query += " r._field == \"" + field + "\"";
                } else {
                    query += " or r._field == \"" + field + "\"";
                }
            }

            query += ")";
        }
        return query;
    }

    /**
     * 聚合查询
     */
    public static String aggrQ(String bucket, String measurement, List<String> fields,
                               String start, String every) {
        String query = baseQ(bucket, measurement, fields, start);
        query += " |> aggregateWindow(every: " + every + ", fn: mean, createEmpty: true)";
        return query;
    }

    public static <T> List<T> queryAggr(InfluxDBClient influxDB,
                                        String bucket, String measurement, Class<T> clazz,
                                        String start, String every) {
        String query = aggrQ(bucket, measurement, null, start, every);
        return doQuery(influxDB, clazz, query);
    }

    /**
     * 通用聚合查询
     */
    public static <T> List<T> queryAggr(InfluxDBClient influxDB,
                                        String bucket, String measurement, List<String> fields, Class<T> clazz,
                                        String start, String every) {
        String query = aggrQ(bucket, measurement, fields, start, every);
        return doQuery(influxDB, clazz, query);
    }

    /**
     * 查询某个 measurement 的某个字段， 整 5 min 的数据，如 10:20， 10:25， 10:30
     */
    public static <T> List<T> queryBy5Min(InfluxDBClient influxDB,
                                          String bucket,
                                          String measurement, Class<T> clazz) {
        return queryBy5Min(influxDB, bucket, measurement, null, clazz);
    }

    public static <T> List<T> queryBy5Min(InfluxDBClient influxDB,
                                          String bucket,
                                          String measurement, List<String> fields, Class<T> clazz) {
        return queryAggr(influxDB, bucket, measurement, fields, clazz, "-1d", "5m");
    }

    public static <T> List<T> queryBy1Min(InfluxDBClient influxDB,
                                          String bucket,
                                          String measurement, Class<T> clazz) {
        return queryBy1Min(influxDB, bucket, measurement, null, clazz);
    }

    public static <T> List<T> queryBy1Min(InfluxDBClient influxDB,
                                          String bucket,
                                          String measurement, List<String> fields, Class<T> clazz) {
        return queryAggr(influxDB, bucket, measurement, fields, clazz, "-1d", "1m");
    }

    /**
     * 查询某个 measurement 的某个字段， 整点数据，如 10:00， 11:00， 12:00
     */
    public static <T> List<T> queryByHour(InfluxDBClient influxDB,
                                          String bucket,
                                          String measurement, Class<T> clazz) {
        return queryByHour(influxDB, bucket, measurement, null, clazz);
    }

    public static <T> List<T> queryByHour(InfluxDBClient influxDB,
                                          String bucket,
                                          String measurement, List<String> fields, Class<T> clazz) {
        return queryAggr(influxDB, bucket, measurement, fields, clazz, "-7d", "1h");
    }

    /**
     * 查询某个 measurement 的某个字段， 整天数据，如 2021-01-01 00:00:00， 2021-01-02 00:00:00
     */
    public static <T> List<T> queryByDay(InfluxDBClient influxDB,
                                         String bucket,
                                         String measurement, Class<T> clazz) {
        return queryByDay(influxDB, bucket, measurement, null, clazz);
    }

    public static <T> List<T> queryByDay(InfluxDBClient influxDB,
                                         String bucket,
                                         String measurement, List<String> fields, Class<T> clazz) {
        return queryAggr(influxDB, bucket, measurement, fields, clazz, "-1M", "1d");
    }

    /**
     * 删除一个月前的数据
     */
    public static void deleteBeforeOneMonth(InfluxDBClient influxDB, String bucket, String org, String measurement) {
        OffsetDateTime now = OffsetDateTime.now();

        influxDB.getDeleteApi().delete(
                now.minusYears(2), now.minusMonths(1),
                String.format("_measurement=\"%s\"", measurement),
                bucket, org
        );
    }

    /**
     * 删除所有数据
     */
    public static void deleteAll(InfluxDBClient influxDB, String bucket, String org, String measurement) {
        OffsetDateTime now = OffsetDateTime.now();

        influxDB.getDeleteApi().delete(
                now.minusYears(100), now,
                String.format("_measurement=\"%s\"", measurement),
                bucket, org
        );
    }

    /**
     * 将 FluxTable 转化为 pojo list
     */
    @SneakyThrows
    public static <T> List<T> fluxTable2Pojo(List<FluxTable> tables, Class<T> clazz) {
        List<T> ts = new ArrayList<>();

        for (FluxTable tab : tables) {
            List<FluxRecord> recs = tab.getRecords();
            for (int i = 0; i < recs.size(); i++) {
                FluxRecord r = recs.get(i);

                T t = null;
                if (i > ts.size() - 1) {
                    t = clazz.newInstance();
                    ts.add(t);
                } else {
                    t = ts.get(i);
                }

                record2Pojo(r, t);
            }
        }

        return ts;
    }

    /**
     * 将 {@link FluxRecord } 转化为 pojo 对象
     */
    public static <T> T record2Pojo(FluxRecord record, T t) {
        try {
            Class clazz = t.getClass();

            Map<String, Object> recordValues = record.getValues();

            String recordField = record.getField();
            Object value = record.getValue();

            Field[] fileds = clazz.getDeclaredFields();
            for (Field field : fileds) {
                Column colAnno = field.getAnnotation(Column.class);

                MeasurementPrefix prefixAnno = field.getAnnotation(MeasurementPrefix.class);
                MeasurementSuffix suffixAnno = field.getAnnotation(MeasurementSuffix.class);
                if (null == colAnno || null != suffixAnno || null != prefixAnno) {
                    continue;
                }

                field.setAccessible(true);

                String colName = colAnno.name();

                if (Objects.equals("time", colName)) {
                    field.set(t, record.getTime());
                } else if (Objects.equals(colName, recordField)) {
                    if (null == value) {
                        continue;
                    }
                    String type = field.getType().getName();

                    if (colName.equals("writeSectTotal")) {
                        System.out.println("writeSectTotal");
                    }

                    if ((type.equals("long") || type.equals("java.lang.Long"))
                            && value instanceof Double) {
                        field.set(t, ((Double) value).longValue());
                    } else if ((type.equals("int") || type.equals("java.lang.Integer"))
                            && value instanceof Double) {
                        field.set(t, ((Double) value).intValue());
                    } else {
                        field.set(t, value);
                    }

                } else {
                    Object v = recordValues.get(field.getName());
                    if (null != v) {
                        field.set(t, v);
                    }
                }
            }

            return t;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 将 {@link FluxRecord } 转化为 pojo 对象
     */
    public static <T> T record2Pojo(FluxRecord record, Class<T> clazz) {
        try {
            T t = clazz.newInstance();

            Map<String, Object> recordValues = record.getValues();

            String recordField = record.getField();
            Object value = record.getValue();

            for (Field field : clazz.getDeclaredFields()) {
                Column colAnno = field.getAnnotation(Column.class);

                MeasurementPrefix prefixAnno = field.getAnnotation(MeasurementPrefix.class);
                MeasurementSuffix suffixAnno = field.getAnnotation(MeasurementSuffix.class);
                if (null == colAnno || null != suffixAnno || null != prefixAnno) {
                    continue;
                }

                field.setAccessible(true);

                String colName = colAnno.name();

                if (Objects.equals("time", colName)) {
                    field.set(t, record.getTime());
                } else if (Objects.equals(colName, recordField)) {
                    field.set(t, value);
                } else {
                    Object v = recordValues.get(field.getName());
                    if (null != v) {
                        field.set(t, v);
                    }
                }
            }

            return t;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}