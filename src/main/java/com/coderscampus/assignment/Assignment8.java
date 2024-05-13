package com.coderscampus.assignment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Assignment8 {
    public static void main(String[] args) {
        Assignment8 assignment8 = new Assignment8();
        assignment8.processNumbers();
    }

    private List<Integer> numbers = null;
    private AtomicInteger i = new AtomicInteger(0);

    public Assignment8() {
        try {
            // Make sure you download the output.txt file for Assignment 8
            // and place the file in the root of your Java project
            numbers = Files.readAllLines(Paths.get("output.txt"))
                    .stream()
                    .map(n -> Integer.parseInt(n))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will return the numbers that you'll need to process from the list
     * of Integers. However, it can only return 1000 records at a time. You will
     * need to call this method 1,000 times in order to retrieve all 1,000,000
     * numbers from the list
     *
     * @return Integers from the parsed txt file, 1,000 numbers at a time
     */
    public List<Integer> getNumbers() {
        int start, end;
        synchronized (i) {
            start = i.get();
            end = i.addAndGet(1000);

            System.out.println("Starting to fetch records " + start + " to " + (end));
        }
        // force thread to pause for half a second to simulate actual Http / API traffic
        // delay
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }

        List<Integer> newList = new ArrayList<>();
        IntStream.range(start, end)
                .forEach(n -> {
                    newList.add(numbers.get(n));
                });
        System.out.println("Done Fetching records " + start + " to " + (end));
        return newList;
    }
    public void processNumbers(){
        int numOfCalls = numbers.size()/1000;
        List<CompletableFuture<List<Integer>>> futures = new ArrayList<>();

        for(int j=0;j<numOfCalls;j++){
            futures.add(CompletableFuture.supplyAsync(this::getNumbers));
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        CompletableFuture<List<List<Integer>>> allPageContentsFuture = allFutures.thenApply(v ->
                futures.stream().map(CompletableFuture::join).collect(Collectors.toList())
        );

        CompletableFuture<Map<Integer,Long>> frequencyMapFuture = allPageContentsFuture.thenApply(lists -> {
            List<Integer> allNumbers = lists.stream().flatMap(List::stream).collect(Collectors.toList());
            return allNumbers.stream().collect(Collectors.groupingBy(num -> num, Collectors.counting()));
        });

        try {
            Map<Integer, Long> frequencyMap = frequencyMapFuture.get();
            frequencyMap.forEach((k,v) -> System.out.println("Number : "+k+" = "+v));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}


