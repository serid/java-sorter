package rs.jit.jift;

import java.lang.IllegalArgumentException;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.Comparator;
import java.util.function.BinaryOperator;

import java.io.IOException;
import java.io.UncheckedIOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.math.RoundingMode;

class Main {
  static String help = """
  sorter sorts lines from each file into three kinds:
  * floating-point number
    Arbitrary precision floating-point number.
    Line shall not have leading or trailing whitespace.
  * integral number
    Arbitrary sized integer number.
    Line shall not have leading or trailing whitespace.
  * string
  
  USAGE:
    sorter [OPTIONS] [PATH ...]
  OPTIONS:
    -o PATH
      Output path. Default is "./".

    -p PREFIX
      Output filename prefix. Default is "".

    -a
      Do not overwrite contents, append.
      
    -s
      Short statistics. Default.

    -f
      Full statistics.""";

  public static void main(String[] args) {
    try {
      System.out.println("Doge sorter9k, ingesting lines:");
      Options options = Options.parse(args);

      if (options.files().length == 0 || Arrays.asList(args).contains("--help")) {
        System.out.println(help);
        return;
      }

      //System.out.println(options);

      Stream<Path> paths = Arrays.stream(options.files())
        .map(Paths::get);

      // For each file, split it into lines and concatenate into one
      // line stream
      Stream<String> lines = paths.flatMap(path -> {
        try {
          return Files.lines(path);
        } catch (IOException e) {
          throw new UncheckedIOException("file not present or is not accessible: " + path.toString(), e);
        }
      });
    
      var results = sort(lines);

      // Finally writeout the results and print statistics
      for (var result : results) {
        // Avoid creating empty files
        if (result.lines.isEmpty()) continue;
        var path = Paths.get(options.outputPath(), options.prefix() + result.name + "s.txt");
        var openOptions = options.appendEh()
          ? new StandardOpenOption[] { StandardOpenOption.APPEND }
          : new StandardOpenOption[0];

        try {
          Files.write(path, result.lines, openOptions);
        } catch (IOException e) {
          throw new UncheckedIOException("unable to create file: " + e.getMessage(), e);
        }

        System.out.printf("* %s sorting statistics:\n", result.name);
        System.out.printf("  count: %s\n", result.lines.size());
        if (options.statisticsType() == StatisticsType.SHORT) continue;
        System.out.printf("  min: %s\n", result.min);
        System.out.printf("  max: %s\n", result.max);
        if (result.name == "string") continue;
        System.out.printf("  sum: %s\n", result.sum);
        System.out.printf("  average: %s\n", result.average);
      }
    } catch (UncheckedIOException | IllegalArgumentException e) {
      System.out.println("sorter: error: " + e.getMessage());
    }
  }

  public static SortingState<?>[] sort(Stream<String> lines) {
    var integers = new SortingState<BigInteger>(
      "integer",
      BigInteger::add,
      BigInteger::compareTo,
      BigInteger.ZERO
    );
    var floats = new SortingState<BigDecimal>(
      "float",
      BigDecimal::add,
      BigDecimal::compareTo,
      BigDecimal.ZERO
    );
    var strings = new SortingState<String>(
      "string",
      (_x, _y) -> null,
      (x, y) -> x.length() - y.length(),
      null
    );

    lines.forEach(line -> {
      System.out.println(line);
      try {
        var integer = new BigInteger(line); // may throw
        integers.lines.add(line);
        integers.updateStatisticsWith(integer);
        return;
      } catch (NumberFormatException _unused) {}

      try {
        var float_ = new BigDecimal(line); // may throw
        floats.lines.add(line);
        floats.updateStatisticsWith(float_);
        return;
      } catch (NumberFormatException _unused) {}
      
      strings.lines.add(line);
      strings.updateStatisticsWith(line);
    });

    // Average of integers is a floating-point number
    integers.average = average(new BigDecimal(integers.sum), integers.lines.size());
    floats.average = average(floats.sum, floats.lines.size());

    return new SortingState<?>[] {
      integers,
      floats,
      strings
    };
  }

  static BigDecimal average(BigDecimal sum, int size) {
    if (size == 0) return null;
    return sum.divide(new BigDecimal(size), RoundingMode.FLOOR);
  }
}

class SortingState<T> {
  public String name;
  public BinaryOperator<T> addition;
  public Comparator<T> comparison;
  public ArrayList<String> lines;
  public T min;
  public T max;
  public T sum;
  public BigDecimal average;

  SortingState(String name, BinaryOperator<T> addition, Comparator<T> comparison, T sum) {
    this.name = name;
    this.addition = addition;
    this.comparison = comparison;
    this.lines = new ArrayList<>();
    this.min = null;
    this.max = null;
    this.sum = sum;
    this.average = null;
  }
  
  void updateStatisticsWith(T x) {
    this.sum = this.addition.apply(this.sum, x);
    if (this.min == null) this.min = x;
    if (this.max == null) this.max = x;
    if (this.comparison.compare(x, this.min) < 0) this.min = x;
    if (this.comparison.compare(x, this.max) > 0) this.max = x;
  }
}

record Options(String outputPath, String prefix, boolean appendEh, StatisticsType statisticsType, String[] files) {
  public static Options parse(String args[]) {
    String outputPath = "./";
    String prefix = "";
    boolean appendEh = false;
    StatisticsType statisticsType = StatisticsType.SHORT;
    ArrayList<String> files = new ArrayList<>();
    
    // Инвариант: i указывает на аргумент, который предстоит обработать
    //System.out.println(args[1]);
    for (var i = 0; i < args.length;)
      switch (args[i++]) {
        case "-o" -> {
          if (i >= args.length) throw new IllegalArgumentException("option -o must be followed by a path");
          outputPath = args[i++];
        }
        case "-p" -> {
          if (i >= args.length) throw new IllegalArgumentException("option -p must be followed by a path");
          prefix = args[i++];
        }
        case "-a" -> appendEh = true;
        case "-s" -> statisticsType = StatisticsType.SHORT;
        case "-f" -> statisticsType = StatisticsType.FULL;
        case String file -> files.add(file);
      }
      
    return new Options(outputPath, prefix, appendEh, statisticsType, files.toArray(new String[0]));
  }
}

enum StatisticsType {
  SHORT, FULL
}
