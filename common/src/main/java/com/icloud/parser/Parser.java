package com.icloud.parser;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;


public interface Parser<TYPE> {
    long NANOS_PER_DAY = 24L * 60L * 60L * 1000000000L;

    TYPE parse(Group group);

    default LocalDateTime fromParquetNanoTime(NanoTime nanoTime) {
        final long nanosSinceMidnight = nanoTime.getTimeOfDayNanos();
        final int julianDay = nanoTime.getJulianDay();
        final long epochSeconds = ((long) julianDay - 2440588L) * 24L * 60L * 60L;
        final long epochDay = epochSeconds * 1000000000L;

        final long nanosOfDay = (nanosSinceMidnight + epochDay) % NANOS_PER_DAY;
        final LocalDate epochDayToLocalDate = LocalDate.ofEpochDay(epochSeconds / (24 * 60 * 60));
        final LocalTime nanosToLocalTime = nanosOfDayToLocalTime(nanosOfDay);

        return LocalDateTime.of(epochDayToLocalDate, nanosToLocalTime);
    }

    // java 8 이라서 private method 불가능...
    default LocalTime nanosOfDayToLocalTime(long nanosOfDay) {
        if (nanosOfDay <= 0) return LocalTime.MIDNIGHT;
        final int nanoOfSecond = (int) (nanosOfDay % 1_000_000_000);
        final long secondsOfDay = nanosOfDay / 1_000_000_000;
        return LocalTime.ofNanoOfDay(secondsOfDay * 1_000_000_000L + nanoOfSecond);
    }
}
