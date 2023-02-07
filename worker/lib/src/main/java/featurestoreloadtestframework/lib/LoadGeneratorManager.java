/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package featurestoreloadtestframework.lib;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.commons.lang3.StringUtils;


public class LoadGeneratorManager<T, R> {
	public enum SAMPLE_STRATEGY {
		IN_ORDER,
		SHUFFLED,
	}

	private List<LoadGenerator<T, R>> workQueue;
	private List<LoadGenerator<T, R>> requestList;
	private int targetQPS;
	private int numberThreads;
	private int numberWarmupSamples;
	private int numberSamples;
	private SAMPLE_STRATEGY sampleStrategy;
	private Sleeper sleeper;

	// TODO: Eventually this should be migrated to 'R'. For now we assume R = FeatureStoreLoadTestResult.
	private Vector<FeatureStoreLoadTestResult> responseStats = new Vector<>();
	private Long seed;
	private String blobLocation = "";
	private String blobBucket = "";
	private String aggregatedResultsPath = "";
	private ChartWriter chartWriter = null;
	private String bqDatasetName = "";
	private String formattedDate = "";
	private String uuid = "";

	final String GsOriginalPathFormat = "^gs://(?<bucket>[^/]+)/?(?<blob>.*)$";
	final String GsFinalPathFormat = "^gs://(?<bucket>[^/]+)/(?<blob>.+)$";
	final int MaxStringLength = 2000000000;

	private LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
		                     int numberWarmupSamples, int numberSamples, String blobLocation,
                             Sleeper sleeper, String projectId, String location, String datasetId) {
		this.targetQPS = targetQPS;
		this.numberThreads = numberThreads;
		this.sampleStrategy = sampleStrategy;
		this.numberWarmupSamples = numberWarmupSamples;
		this.numberSamples = numberSamples;
		this.sleeper = sleeper;
		this.bqDatasetName = datasetId;
		if (StringUtils.isNotBlank(blobLocation)) {
			this.chartWriter = new ChartWriter(projectId, location);
			this.getGCSFilePaths(blobLocation);
		}
	}

	private void getGCSFilePaths(String blobLocation) {
		// Validate format and split bucket from path
		if (!Pattern.matches(GsOriginalPathFormat, blobLocation)){
			throw new IllegalArgumentException(String.format(
				"Invalid GCS path: `%s`", blobLocation));
		}
		if (!blobLocation.endsWith("/")) {
			blobLocation += "/";
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss", Locale.US);
		this.formattedDate = sdf.format(new Date());
		this.uuid = UUID.randomUUID().toString();
		String aggregatedResult = blobLocation + String.format("aggregated_results_%s_%s.txt",
						this.formattedDate, this.uuid);
		Matcher aggregatedResultMatcher = Pattern.compile(GsFinalPathFormat).matcher(aggregatedResult);
		aggregatedResultMatcher.find();
		this.blobBucket = aggregatedResultMatcher.group("bucket");
		this.aggregatedResultsPath = aggregatedResultMatcher.group("blob");
		if (this.bqDatasetName == "") {
			// BQ dataset name must be alphanumeric w/underscores.
			String uuidWithoutDashes = this.uuid.replace('-', '_');
			this.bqDatasetName = String.format("vertex_ai_benchmarker_results_%d_qps_%s", this.targetQPS,
					uuidWithoutDashes);
		}
		this.blobLocation = blobLocation;
	}

	public LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
								int numberWarmupSamples, int numberSamples, String blobLocation,
								LoadGeneratorRequestListBuilder builder) {
		this(targetQPS, numberThreads, sampleStrategy, numberWarmupSamples, numberSamples,
             blobLocation, new ThreadSleeper(), "", "", "");
		this.requestList = builder.generateRequestList();
	}

	public LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
								int numberWarmupSamples, int numberSamples, String blobLocation,
								LoadGeneratorRequestListBuilder builder, String projectId, String location) {
		this(targetQPS, numberThreads, sampleStrategy, numberWarmupSamples, numberSamples,
             blobLocation, new ThreadSleeper(), projectId, location, "");
		this.requestList = builder.generateRequestList();
	}

	public LoadGeneratorManager(int targetQPS, int numberThreads, SAMPLE_STRATEGY sampleStrategy,
								int numberWarmupSamples, int numberSamples, String blobLocation,
								LoadGeneratorRequestListBuilder builder, String projectId, String location, String datasetId) {
		
		this(targetQPS, numberThreads, sampleStrategy, numberWarmupSamples, numberSamples,
             blobLocation, new ThreadSleeper(), projectId, location, "");
        this.seed = seed;
        this.requestList = builder.generateRequestList();
    }

	private List<LoadGenerator<T, R>> generateWorkQueue(SAMPLE_STRATEGY sampleStrategy) {
		switch (sampleStrategy) {
			case IN_ORDER:
				return this.requestList;
			case SHUFFLED:
				List<LoadGenerator<T, R>> t = new ArrayList<>(requestList.size());
				List<LoadGenerator> temp = new LinkedList<>();
				temp.addAll(requestList);
				Random r = seed == null ? new Random() : new Random(seed);
				while (!temp.isEmpty()) {
					int nextElemIdx = r.nextInt(temp.size());
					LoadGenerator nextElem = temp.remove(nextElemIdx);
					t.add(nextElem);
				}
				assert(t.size() == this.requestList.size());
				return t;
			default:
				throw new RuntimeException(
						"Please define an implementation for the sample strategy '" + sampleStrategy + "'.");
		}
	}

	Logger logger = Logger.getAnonymousLogger();

	/**
	 * Write a string to a GCS file.
	 * @param filePath The GCS file path.
	 * @param content The string to write.
	 */
	private void writeStrToGcs(String filePath, String content) {
		if (this.blobBucket.isEmpty()) {
			logger.fine("Blob bucket is empty - is the output GCS Path set?");
			return;
		}

		if (this.chartWriter == null) {
			logger.fine("BQ writer was not initialized - will not write to BQ.");
			return;
		}
		this.chartWriter.write(this.blobBucket, filePath, content);
	}

	private String getNewBlobAndTableNames(int detailedResultIndex) {
		String detailResult = this.blobLocation + String.format("detailed_results_%s_%s_%d.csv",
					this.formattedDate, this.uuid, detailedResultIndex);
		Matcher detailResultMatcher = Pattern.compile(GsFinalPathFormat).matcher(detailResult);
		detailResultMatcher.find();
		String detailResultPath = detailResultMatcher.group("blob");
		return detailResultPath;
	}

	private void WriteContent(WritableByteChannel channel, StringBuilder content, String bqTableName, String detailResultPath, WriteDisposition option) throws InterruptedException, IOException {
		try {
			channel.write(ByteBuffer.wrap(content.toString().getBytes(StandardCharsets.UTF_8)));
		} catch (IOException e) {
			System.err.println("Failed to write new content to detailed results csv file: " + e.getMessage());
			throw e;
		}

		channel.close();

		this.chartWriter.exportToBQ(this.bqDatasetName, bqTableName, String.format("gs://%s/%s", this.blobBucket, detailResultPath), option);
	}

	private void writeFullResultToCSV() throws IOException, BigQueryException, InterruptedException {
		String csvHeader = "StartTime,Duration\n";
		if (this.chartWriter == null) {
			return;
		}
		WriteDisposition option = WriteDisposition.WRITE_TRUNCATE;
		String bqTableName = String.format("loadtest_result_table_%s_%s", formattedDate, uuid);

		int detailedResultIndex = 1;

		String detailResultPath = this.getNewBlobAndTableNames(detailedResultIndex);
		Blob detailedBlob = this.chartWriter.write(this.blobBucket, detailResultPath, "");

		StringBuilder content = new StringBuilder(csvHeader);
		WritableByteChannel channel = detailedBlob.writer();

		for (FeatureStoreLoadTestResult result : this.responseStats) {
			content.append(result.toString() + "\n");

			// Write to csv file and append to BQ table before string exceeds Java max string length.
			if (content.length() > this.MaxStringLength) {
				this.WriteContent(channel, content, bqTableName, detailResultPath, option);
				detailedResultIndex ++;

				detailResultPath = this.getNewBlobAndTableNames(detailedResultIndex);
				detailedBlob = this.chartWriter.write(this.blobBucket, detailResultPath, "");

				content = new StringBuilder(csvHeader);
				channel = detailedBlob.writer();
				option = WriteDisposition.WRITE_APPEND;
			}
		}

		this.WriteContent(channel, content, bqTableName, detailResultPath, option);
	}

	private void runSamples(int numSamples, boolean keepStats) throws InterruptedException {
		ExecutorService pool = Executors.newCachedThreadPool();

		AtomicInteger numberExceededTime = new AtomicInteger();
		int index = 0;
		for (int sampleNum = 0; sampleNum < numSamples; sampleNum++) {
			long start = System.currentTimeMillis();
			long stop = start  + 1000;
			int paramSampleNum;

			// Freeze values for Runnable.
			final int _index = index;
			final int _sampleNum = sampleNum;
			final long _stop = stop;
			Future<?> future = pool.submit(() -> {
				try {
					if (!runSample(_index, keepStats)) {
						System.out.println("[Sample " + _sampleNum + "] Failed.");
					}
					long end = System.currentTimeMillis();
					if (end > _stop) {
						System.out.println(
								"[Sample " + _sampleNum + "] Unable to reach desired QPS.");
						numberExceededTime.incrementAndGet();
					} else {
						System.out.println("[Sample " + _sampleNum + "] Reached target QPS.");
					}
				} catch (Exception e) {
					System.out.println("[Sample " + _sampleNum + "] Exception: " + e);
					e.printStackTrace();
				}
			});

			index += targetQPS;
			index = index % workQueue.size();

			try {
				sleeper.sleep(stop - System.currentTimeMillis());
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		pool.shutdown();
		if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
			System.out.println("Tasks are still pending!");
		}
	}

	private void runExperiment() throws InterruptedException, IOException, BigQueryException {

		System.out.println("Running warmup samples.");
		runSamples(this.numberWarmupSamples, false); // Don't keep stats for warmup samples.
		System.out.println();
		System.out.println("Running samples.");
		runSamples(this.numberSamples, true); // Record stats for samples after warmup is done.

		// Write aggregate to console. And also write to a GCS file if a GCS output path was given.
		String statString = CalculateStats();
		System.out.println(statString);
		writeStrToGcs(this.aggregatedResultsPath, statString);
		writeFullResultToCSV();
	}

	private Duration percentile(List<Duration> stats, double percentile) {
		int index = (int) Math.ceil(percentile / 100.0 * stats.size());
    		return stats.get(index-1);
	}

	private Duration interpolation(List<Duration> sortedStats, double percentile) {
		int floor = (int) Math.floor(percentile * (sortedStats.size() - 1) / 100);
		Duration y0 = sortedStats.get(floor);

		Duration y1 = sortedStats.get(floor + 1);
		double perBucket = 100.0D / (sortedStats.size() - 1);

		// interpolation = (x - x0) * (y1 - y0) + y0
		double difference = (percentile - (perBucket * floor)) / perBucket;
		// (x - x0) * (y1 - y0). Duration methods can only multiply by a long - so convert to nanos,
		// multiply by the double, and convert back.
		Duration yDiff = y1.minus(y0);
		Duration interpol = Duration.ofNanos(
				Math.round(difference * yDiff.toNanos()));
		interpol = interpol.plus(y0);  // + y0
		return interpol;
	}

	private String CalculateStats() {
		if (responseStats.isEmpty()) {
			System.out.println("No stats to calculate yet!");
			return StringUtils.EMPTY;
		}
		double average = 0.0D;
		int numErrors = 0;
		int numRequests = this.responseStats.size();
		List<Duration> sortedDurations = new ArrayList<>(numRequests);
		for (FeatureStoreLoadTestResult curr : this.responseStats) {
			// If a code is present that means an error occurred.
			if (curr.getErrorCode().isPresent()) {
				numErrors++;
				continue;
			}
			long durationInMillis = curr.getLatency().toMillis();
			average += durationInMillis;
			sortedDurations.add(curr.getLatency());
		}
		int numValidRequests = numRequests - numErrors;
		average = average / numValidRequests;

		Collections.sort(sortedDurations);

		// In case all requests return as error - set all values to NaN.
		double min = Double.NaN;
		double max = Double.NaN;
		double p90 = Double.NaN;
		double p95 = Double.NaN;
		double p99 = Double.NaN;
		if (sortedDurations.size() > 0) {
			min = DurationUtils.toMillis(sortedDurations.get(0));
			max = DurationUtils.toMillis(sortedDurations.get(sortedDurations.size() - 1));
			p90 = DurationUtils.toMillis(interpolation(sortedDurations, 90));
			p95 = DurationUtils.toMillis(interpolation(sortedDurations, 95));
			p99 = DurationUtils.toMillis(interpolation(sortedDurations, 99));
		}
		double errorPercentage = 100.0D * (((double) numErrors) / this.responseStats.size());

		return String.format(
				"(numbers in ms) Min: %.2f, Max: %.2f, Average: %.2f, P90: %.2f, P95: %.2f, P99: %.2f, Error percentage: %.2f%%\n",
				min, max, average, p90, p95, p99, errorPercentage);
	}

	private void verifyBucketExists() throws Exception {
		Storage storage = StorageOptions.getDefaultInstance().getService();
		Bucket bucket = storage.get(this.blobBucket);
		if (bucket == null) {
			throw new Exception(String.format("Unable to find bucket `%s`", this.blobBucket));
		}
	}

	/**
	 * Collect return values from finished Futures and places them into this.responseStats.
	 * @param futures The futures to look through - they may be in different states, either finished,
	 *   				      cancelled or running.
	 */
	private void collectFutureResultValues(List<Future<R>> futures) {
		// Iterate through all futures in the list.
		for (Future future : futures) {
			// If the future is not finished or was cancelled then the result is not useful.
			if (!future.isDone() || future.isCancelled()) {
				continue;
			}

			// Collect the result.
			try {
				R response = ((Future<R>) future).get();
				this.responseStats.add((FeatureStoreLoadTestResult) response);
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e); // Since future is finished, should not get here.
			}
		}
	}

	/**
	 * Will use items from the workQueue from index to index+targetQps % workQueue.length.
	 * TODO: Return a percentage/stats about how many requests in the sample succeeded.
	 * @param startIndex Which index in the work queue should sample start at?
	 * @param keepStats Should the statistics be calculated? This is useful to turn off when running
	 *                  initial samples which may be slow due to gRPC channel creation, other cache
	 *                  warming, etc.
	 * @return If the sample successfully completed.
	 */
	private boolean runSample(int startIndex, boolean keepStats) {
		ExecutorService executor = Executors.newFixedThreadPool(this.numberThreads);
		List<Future<R>> futures = new ArrayList<>(targetQPS);
		for (int x = 0; x < targetQPS; x++) {
			int workQueueIdx = (startIndex + x) % workQueue.size();
			LoadGenerator<T, R> workItem = workQueue.get(workQueueIdx);
			Future<R> future = executor.submit(workItem);
			futures.add(future);
		}
		try {
			executor.shutdown();
			if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
				executor.shutdownNow();
				if (keepStats) {
					collectFutureResultValues(futures);
				}
				return false;
			}
		} catch(InterruptedException e) {
			executor.shutdownNow();
			if (keepStats) {
				collectFutureResultValues(futures);
			}
			return false;
		}

		if (keepStats) {
			collectFutureResultValues(futures);
		}
		return true;
	}

	protected List<LoadGenerator<T, R>> getWorkQueue() {
		return Collections.unmodifiableList(workQueue);
	}

	public void run() throws WorkTimeoutException, Exception, BigQueryException {
		if (!this.blobBucket.isEmpty()) {
			verifyBucketExists();
		}
		workQueue = Collections.unmodifiableList(generateWorkQueue(sampleStrategy));
		try {
			runExperiment();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
