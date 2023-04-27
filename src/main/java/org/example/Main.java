package org.example;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;

import com.google.api.core.ApiFuture;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class Main {

  private static final File KEY_CACHE_FILE = new File("keys.txt");

  private static final int CONCURRENCY = 200;
  private static int targetQps;

  private static String tableId;

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    if (args.length != 4) {
      throw new IllegalArgumentException("Wrong number of args.");
    }
    String projectId = args[0];
    String instanceId = args[1];
    tableId = args[2];
    targetQps = Integer.parseInt(args[3]);

    System.out.println(
        String.format("Start benchmarking using project=%s instance=%s table=%s", projectId,
            instanceId, tableId));

    BigtableDataSettings.enableBuiltinMetrics();

    BigtableDataSettings.Builder settings = BigtableDataSettings.newBuilder()
        .setProjectId(projectId)
        .setInstanceId(instanceId);

    settings.stubSettings()
        .setTransportChannelProvider(
            EnhancedBigtableStubSettings.defaultGrpcTransportProviderBuilder()
                .setChannelPoolSettings(
                    ChannelPoolSettings.builder()
                        .setMinRpcsPerChannel(1)
                        .setMaxRpcsPerChannel(10)
                        .setInitialChannelCount(1)
                        .setMinChannelCount(1)
                        .setMaxChannelCount(20)
                        .setPreemptiveRefreshEnabled(true)
                        .build()
                ).setAttemptDirectPath(false).build()
        );
    BigtableDataClient client = BigtableDataClient.create(settings.build());
    System.out.println("Getting row keys");
    List<ByteString> randomKeys = getCachedKeys(client);
    System.out.printf("Got %d row keys%n", randomKeys.size());

    // Target a steady QPS
    @SuppressWarnings("UnstableApiUsage")
    RateLimiter rateLimiter = RateLimiter.create(targetQps);

    // Since we are using the blocking variant of readRows, we need some parallelism to hit the
    // target QPS
    ExecutorService executor = Executors.newCachedThreadPool();

    System.out.println("Starting load");
    for (int i = 0; i < CONCURRENCY; i++) {
      executor.submit(new Worker(client, randomKeys, rateLimiter));
    }
  }

  static class Worker implements Runnable {

    private final BigtableDataClient client;
    private final List<ByteString> rowKeys;
    @SuppressWarnings("UnstableApiUsage")
    private final RateLimiter rateLimiter;

    public Worker(BigtableDataClient client, List<ByteString> rowKeys, RateLimiter rateLimiter) {
      this.client = client;
      this.rowKeys = rowKeys;
      this.rateLimiter = rateLimiter;
    }

    private final Random random = new Random();

    @Override
    public void run() {
      while (true) {
        rateLimiter.acquire();
        int keyI = random.nextInt(rowKeys.size());
        try {
          Filters.Filter filter = FILTERS.limit().cellsPerRow(1);
          client.readRow(tableId, rowKeys.get(keyI), filter);
        } catch (RuntimeException e) {
          if (Thread.interrupted()) {
            System.out.println("Worker interrupted, exiting");
            return;
          }
          System.out.println(e);
        }
      }
    }
  }

  /**
   * Try to get a list a sampling of the keys in the table. The keys will be read from
   * KEY_CACHE_FILE if it exists. Otherwise they will be fetched from the table.
   *
   * This os only used for setting up the benchmark.
   */
  static List<ByteString> getCachedKeys(BigtableDataClient client)
      throws IOException, ExecutionException, InterruptedException {
    // Try to read the cached keys
    if (KEY_CACHE_FILE.exists()) {
      try (BufferedReader fin = new BufferedReader(new FileReader(KEY_CACHE_FILE))) {
        Decoder decoder = Base64.getDecoder();

        String line = fin.readLine();
        ImmutableList.Builder<ByteString> builder = ImmutableList.builder();

        while (line != null) {
          builder.add(ByteString.copyFrom(decoder.decode(line)));
          line = fin.readLine();
        }
        return builder.build();
      }
    }

    System.out.println(
        "Cached lookup keys not found, fetching them, this will take a little while");
    // If the keys werent cached, then fetch them from bigtable and cache them to speed up the actual
    // benchmark setup time
    List<ByteString> results = getRandomKeys(client);
    // List<ByteString> results = getAllKeys(client);

    try (FileWriter fout = new FileWriter(KEY_CACHE_FILE)) {
      Encoder encoder = Base64.getEncoder();
      for (ByteString result : results) {
        fout.write(encoder.encodeToString(result.toByteArray()));
        fout.write("\n");
      }
    }
    return results;
  }

  /**
   * Fetch a sample of keys from across all the tablets. This os only used for setting up the
   * benchmark.
   */
  static List<ByteString> getRandomKeys(BigtableDataClient client)
      throws ExecutionException, InterruptedException {
    System.out.println("Sampling keys in the table");

    // Fetch the tablet boundaries.
    List<KeyOffset> keyOffsets = client.sampleRowKeys(tableId);
    System.out.println("Found " + keyOffsets.size() + " tablets.");

    ByteString startKey = ByteString.EMPTY;
    if (keyOffsets.get(0).getKey().isEmpty()) {
      keyOffsets = keyOffsets.subList(1, keyOffsets.size() - 1);
    }

    // Limit parallelism of concurrent requests
    Semaphore semaphore = new Semaphore(CONCURRENCY);

    List<ApiFuture<List<Row>>> results = new ArrayList<>();

    // Fetch a sample of 10 keys per tablet
    for (KeyOffset keyOffset : keyOffsets) {
      System.out.println("startKey=" + startKey + " endKey=" + keyOffset.getKey());
      semaphore.acquire();
      Query q = Query.create(tableId)
          .range(startKey, keyOffset.getKey())
          .filter(
              // randomly sample keys in a tablet
              // strip out all the cells, we only care about the key
              FILTERS.chain()
                  .filter(FILTERS.key().sample(0.01))
                  .filter(FILTERS.limit().cellsPerRow(1))
                  .filter(FILTERS.value().strip())

          )
          .limit(10);

      startKey = keyOffset.getKey();

      // Asynchronously fetch the keys and release the semaphore
      ApiFuture<List<Row>> f = client.readRowsCallable().all().futureCall(q);
      f.addListener(
          semaphore::release,
          MoreExecutors.directExecutor()
      );
      results.add(f);
    }

    // Await and flatten the futures
    List<ByteString> resultKeys = new ArrayList<>();
    for (ApiFuture<List<Row>> result : results) {
      resultKeys.addAll(
          result.get().stream().map(Row::getKey).collect(Collectors.toList())
      );
    }
    for (ByteString key : resultKeys) {
      System.out.println("key=" + key);
    }
    System.out.println("Done sampling keys in the table");
    return resultKeys;
  }

  static List<ByteString> getAllKeys(BigtableDataClient client)
      throws ExecutionException, InterruptedException {
    System.out.println("Get all keys in the table");

    // Limit parallelism of concurrent requests
    Semaphore semaphore = new Semaphore(CONCURRENCY);

    Query q = Query.create(tableId)
        .filter(FILTERS.chain()
            .filter(FILTERS.key().sample(0.01))
            .filter(FILTERS.limit().cellsPerColumn(1))
            .filter(FILTERS.value().strip()));
    // .limit(100);
    ServerStream<Row> stream = client.readRows(q);
    List<ByteString> resultKeys = new ArrayList<>();

    for (Row row : stream) {
      resultKeys.add(row.getKey());
    }

    System.out.println("Done getting all keys in the table");
    return resultKeys;
  }
}