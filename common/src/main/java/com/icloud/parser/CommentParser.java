package com.icloud.parser;

import com.icloud.model.Comment;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.api.Binary;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class CommentParser implements Parser<Comment> {
    private static final long NANOS_PER_DAY = 24L * 60L * 60L * 1000000000L;

    @Override
    public Comment parse(Group group) {
        int Id = group.getInteger("Id", 0);
        Binary binary = group.getInt96("CreationDate", 0);
        NanoTime nanoTime = NanoTime.fromBinary(binary);
        LocalDateTime CreationDate = fromParquetNanoTime(nanoTime);
        int PostId = group.getInteger("PostId", 0);
        int Score = group.getInteger("Score", 0);
        String Text = group.getString("Text", 0);
        Integer UserId;
        try { //TODO 이 부분은 도저히 어쩔 수가 없음...
            UserId = group.getInteger("UserId", 0);
        } catch (Exception e) {
            UserId = null;
        }

        return new Comment(Id, CreationDate, PostId, Score, Text, UserId);
    }

    private static LocalDateTime fromParquetNanoTime(NanoTime nt) {
        long nanosSinceMidnight = nt.getTimeOfDayNanos();
        int julianDay = nt.getJulianDay();
        long epochSeconds = ((long) julianDay - 2440588L) * 24L * 60L * 60L;
        long epochDay = epochSeconds * 1000000000L;

        long nanosOfDay = (nanosSinceMidnight + epochDay) % NANOS_PER_DAY;
        LocalDate epochDayToLocalDate = LocalDate.ofEpochDay(epochSeconds / (24 * 60 * 60));
        LocalTime nanosToLocalTime = nanosOfDayToLocalTime(nanosOfDay);

        return LocalDateTime.of(epochDayToLocalDate, nanosToLocalTime);
    }

    private static LocalTime nanosOfDayToLocalTime(long nanosOfDay) {
        if (nanosOfDay <= 0) return LocalTime.MIDNIGHT;
        int nanoOfSecond = (int) (nanosOfDay % 1_000_000_000);
        long secondsOfDay = nanosOfDay / 1_000_000_000;
        return LocalTime.ofNanoOfDay(secondsOfDay * 1_000_000_000L + nanoOfSecond);
    }
}
