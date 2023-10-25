package com.example.influxdbormdemo.anno;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * measurement 后缀
 * 与 @Measurement 拼接 动态的 measurement
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface MeasurementSuffix {
}
