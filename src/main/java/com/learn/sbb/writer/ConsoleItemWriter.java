package com.learn.sbb.writer;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ConsoleItemWriter extends AbstractItemStreamItemWriter {
    @Override
    public void write(List items) throws Exception {
        items.stream().forEach(System.out::println);
        System.out.println("************** writing each chunk ****************");
    }
}
