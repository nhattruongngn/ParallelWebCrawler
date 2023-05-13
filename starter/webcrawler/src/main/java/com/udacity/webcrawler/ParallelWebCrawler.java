package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final List<Pattern> patternList;
  private final int maxDepth;
  private final PageParserFactory parserFactory;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          @MaxDepth int maxDepth,
          @Timeout Duration timeout,
          @IgnoredUrls List<Pattern> patternList,
          PageParserFactory parserFactory,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int parallelism) {
    this.maxDepth = maxDepth;
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.pool = new ForkJoinPool(Math.min(parallelism, getMaxParallelism()));
    this.popularWordCount = popularWordCount;
    this.patternList = new ArrayList<>(patternList);
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    ConcurrentMap<String, Integer> wordCounts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();
    startingUrls.forEach(url -> pool.invoke(new WebCrawlerTask(url, clock.instant().plus(timeout), maxDepth, wordCounts, visitedUrls, new ArrayList<>(patternList))));
    if (!wordCounts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(WordCounts.sort(wordCounts, popularWordCount))
              .setUrlsVisited(visitedUrls.size())
              .build();
    } else {
      return new CrawlResult.Builder()
              .setWordCounts(wordCounts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }
  }
  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
  public class WebCrawlerTask extends RecursiveTask<Boolean> {
    private int maxDepth;
    private ConcurrentMap<String, Integer> wordCounts;
    private Instant deadline;
    private String url;
    private ConcurrentSkipListSet<String> visitedUrls;
    private List<Pattern> patternList;

    public WebCrawlerTask(String url,
                          Instant deadline,
                          int maxDepth,
                          ConcurrentMap<String, Integer> wordCounts,
                          ConcurrentSkipListSet<String> visitedUrls,
                          List<Pattern> patternList) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.wordCounts = wordCounts;
      this.visitedUrls = visitedUrls;
      this.patternList = patternList;
    }

    @Override
    protected Boolean compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)
              || patternList.stream().anyMatch(pattern -> pattern.matcher(url).matches())) {
        return false;
      }
      if (!visitedUrls.add(url)) {
        return false;
      }
      PageParser.Result result = parserFactory.get(url).parse();

      for (Map.Entry<String, Integer> entry : result.getWordCounts().entrySet()) {
        wordCounts.compute(entry.getKey(), (key, value) -> (value == null) ? entry.getValue() : entry.getValue() + value);
      }
      List<WebCrawlerTask> tasks = result.getLinks()
              .stream()
              .map(link -> new WebCrawlerTask(link, deadline, maxDepth - 1, wordCounts, visitedUrls, patternList))
              .collect(Collectors.toList());
      invokeAll(tasks);

      int incompleteTaskCount = 0;
      for (WebCrawlerTask task : tasks) {
        if (!task.isCompletedNormally()) {
          incompleteTaskCount++;
        }
      }
      return incompleteTaskCount == 0;
    }
  }
}
