# Chart Analysis Transform - Refactoring Guide

## Overview

This document describes the refactoring of the chart analysis transform from a nested parallelism approach to a Spark-native explode-based architecture.

## Critical Issues Fixed

### 🔴 Blocking Issues (Would Cause Runtime Errors)

1. **Undefined `has_image` Column**
   - **Original Issue**: Line 213 referenced `has_image` column that was never created
   - **Fix**: Added proper image detection in `extract_images_to_rows()` function
   ```python
   markdowns_with_images = markdowns_with_images.withColumn(
       "has_image",
       F.size(col("extracted_images")) > 0
   )
   ```

2. **Unused Timestamp UDF**
   - **Original Issue**: `merge_timestamp_udf` was defined but never applied to the dataframe
   - **Fix**: Created `add_timestamp_tracking()` function that properly applies the UDF
   ```python
   df = df.withColumn("timestamp", merge_udf(col("timestamp")))
   ```

3. **Nested Parallelism Anti-Pattern**
   - **Original Issue**: ThreadPoolExecutor inside Spark UDF causing:
     - Resource contention between Spark and thread pools
     - Unpredictable number of concurrent LLM calls (up to 40+)
     - Memory pressure and executor hangs
   - **Fix**: Removed ThreadPoolExecutor entirely, let Spark handle all parallelism

---

## Architecture Comparison

### Original Architecture (Problematic)

```
Input DataFrame (Pages)
  ↓
  UDF: analyse_page (runs per page)
    ↓
    Extract all images in page → Dictionary
    ↓
    ThreadPoolExecutor (max_workers=5)
      ↓
      5 parallel LLM calls per page
    ↓
    Replace images in markdown
  ↓
Output DataFrame (Pages)
```

**Problems:**
- Nested parallelism: Spark executors × ThreadPool threads
- Heavy UDF with regex, threads, LLM calls, memory management
- Two passes over text with same regex
- Difficult to track errors per image
- Memory issues from multiple large base64 strings

### Refactored Architecture (Spark-Native)

```
Input DataFrame (Pages)
  ↓
Step 1: Extract Images → DataFrame (Images)
  │   One row per image
  │   Spark parallelizes naturally
  ↓
Step 2: Process Images → DataFrame (Images + Analysis)
  │   Simple UDF: one image → one analysis
  │   Spark distributes across executors
  │   Controlled parallelism
  ↓
Step 3: Join & Reconstruct → DataFrame (Pages)
  │   Group by page_id
  │   Replace images with analysis
  ↓
Step 4: Timestamp Tracking → DataFrame (Pages)
  ↓
Output DataFrame (Pages)
```

**Benefits:**
- Single level of parallelism (Spark only)
- Lightweight UDFs
- Better error tracking per image
- Predictable resource usage
- Better memory management

---

## Detailed Changes

### 1. Image Extraction (`extract_images_to_rows`)

**What it does:**
- Compiles regex pattern ONCE (not per UDF call)
- Extracts all images from markdown
- Creates one row per image
- Validates image sizes upfront
- Adds `has_image` flag (FIX #1)

**Key improvements:**
```python
# Regex compiled once at module level
IMAGE_PATTERN = re.compile(r'!\[([^\]]*)\]\(data:image/(jpeg|png);base64,([^)]+)\)')

# Better size validation
estimated_size = len(base64_data) * 0.75
if estimated_size > MAX_IMAGE_SIZE_BYTES:
    continue  # Skip, don't process
```

### 2. Image Processing (`process_images_with_llm`)

**What it does:**
- Simple UDF: one image → one analysis
- Spark distributes images across executors
- Each executor processes ONE image at a time
- Retry logic with exponential backoff
- Proper error tracking

**Key improvements:**
```python
# NO ThreadPoolExecutor!
# Spark handles parallelism naturally
analyzed_df = images_df.withColumn(
    "analysis",
    analyze_udf(col("image_base64"), col("image_type"))
)

# Track processing status
.withColumn("processing_status",
    when(col("analysis").startswith("Error"), "failed")
    .otherwise("success")
)
```

### 3. Markdown Reconstruction (`reconstruct_markdown_with_analysis`)

**What it does:**
- Groups analyzed images by page
- Joins back to original markdown
- Replaces image references with analysis
- Single pass through text

**Key improvements:**
```python
# Group by page_id
images_by_page = analyzed_images_df.groupBy("page_id").agg(...)

# Join instead of nested loops
joined_df = markdowns_with_id.join(images_by_page, ...)

# Simple replacement UDF
result = markdown_text.replace(original, replacement_text)
```

### 4. Timestamp Tracking (`add_timestamp_tracking`)

**What it does:**
- Merges new timestamp with existing timestamps
- Properly applies UDF (FIX #2)
- Logs timestamp addition

**Key improvements:**
```python
# Actually apply the UDF!
df = df.withColumn("timestamp", merge_udf(col("timestamp")))
logger.info(f"Added timestamp for stage '{current_stage}'")
```

---

## Performance Comparison

| Metric | Original | Refactored | Impact |
|--------|----------|------------|--------|
| **Parallelism Model** | Nested (Spark + Threads) | Single (Spark only) | ✅ Predictable |
| **Concurrent LLM Calls** | 8 executors × 5 threads = 40+ | 8 executors × 1 = 8 | ✅ Controlled |
| **Memory Pressure** | High (multiple images/executor) | Low (one image/executor) | ✅ Reduced OOM |
| **Error Tracking** | Per page | Per image | ✅ Granular |
| **Regex Compilation** | Per UDF call | Once at module load | ✅ Faster |
| **Text Scanning** | 2 passes | 1 pass | ✅ Faster |
| **UDF Complexity** | Very heavy | Lightweight | ✅ Better perf |

---

## Migration Guide

### Option 1: Direct Replacement

1. **Backup original code**
   ```bash
   cp original_transform.py original_transform_backup.py
   ```

2. **Replace with refactored version**
   ```bash
   cp chart_analysis_transform_refactored.py your_transform.py
   ```

3. **Update input/output dataset RIDs** (lines 74-76)
   ```python
   @transform(
       mds=Input("YOUR_INPUT_RID"),
       md_output=Output("YOUR_OUTPUT_PATH"),
       model=GenericVisionCompletionLanguageModelInput("YOUR_MODEL_RID")
   )
   ```

4. **Test on small dataset first**
   - Uncomment test limit in refactored code
   - Run on 5-10 pages
   - Verify output quality

### Option 2: Gradual Migration

1. **Run both transforms in parallel**
   - Keep original running
   - Deploy refactored version to new output dataset
   - Compare results

2. **Validate outputs match**
   - Check image analysis quality
   - Verify markdown reconstruction
   - Compare performance metrics

3. **Switch traffic**
   - Point downstream consumers to new dataset
   - Monitor for issues
   - Deprecate original transform

---

## Testing Checklist

- [ ] Test with pages containing 0 images (should pass through unchanged)
- [ ] Test with pages containing 1 image
- [ ] Test with pages containing multiple images (5-10)
- [ ] Test with large images (>5MB) - should skip with warning
- [ ] Test with mixed JPEG and PNG images
- [ ] Test error handling (invalid base64, API failures)
- [ ] Test retry logic (simulate transient failures)
- [ ] Test timestamp merging (existing + new timestamps)
- [ ] Verify memory usage stays stable
- [ ] Check Spark UI for task distribution

---

## Monitoring Recommendations

### Key Metrics to Track

1. **Processing Rate**
   - Images per second
   - Pages per minute
   - Track before/after refactoring

2. **Error Rates**
   - Failed image analysis %
   - Error categories (size, API, timeout)
   - Retry success rate

3. **Resource Usage**
   - Executor memory utilization
   - Shuffle size (should be minimal)
   - GC time percentage

4. **LLM API Metrics**
   - Concurrent requests
   - Average latency
   - Rate limit hits

### Spark UI Checks

1. **Stages**
   - Should see clear stages for: extract, process, join
   - No skewed tasks
   - Even distribution

2. **Tasks**
   - Task duration should be consistent
   - No stragglers
   - Good parallelism

3. **Memory**
   - No spills to disk
   - GC time < 10%
   - Steady executor memory

---

## Troubleshooting

### Issue: "No images to process" but images exist

**Cause**: Regex pattern not matching image format
**Fix**: Check image syntax in markdown, verify regex pattern

### Issue: Out of memory errors

**Cause**: Images too large or too many per page
**Fix**: Reduce `MAX_IMAGES_PER_PAGE` or `MAX_IMAGE_SIZE_MB`

### Issue: LLM API rate limiting

**Cause**: Too many concurrent requests
**Fix**: Reduce `NUM_EXECUTORS` in config or add rate limiting

### Issue: Analysis quality poor

**Cause**: Prompt not optimized for image type
**Fix**: Adjust `VISION_PROMPT` constant

### Issue: Slow performance

**Cause**: Too few executors or network latency
**Fix**: Increase `NUM_EXECUTORS` or check network

---

## Future Enhancements

1. **Caching**
   - Cache analyzed images by hash
   - Skip re-analysis of duplicate images

2. **Batching**
   - Batch multiple images per LLM call (if API supports)
   - Reduce API overhead

3. **Adaptive Sizing**
   - Dynamically adjust partitions based on image count
   - Optimize for both small and large datasets

4. **Quality Metrics**
   - Track analysis quality scores
   - Flag low-confidence analyses

5. **Progressive Processing**
   - Process high-priority images first
   - Implement priority queuing

---

## Code Review Summary

### Issues Fixed

| # | Issue | Severity | Status |
|---|-------|----------|--------|
| 1 | Undefined `has_image` column | 🔴 Critical | ✅ Fixed |
| 2 | Unused timestamp UDF | 🔴 Critical | ✅ Fixed |
| 3 | Nested parallelism (ThreadPoolExecutor) | 🔴 Critical | ✅ Fixed |
| 4 | Expensive `.count()` call | 🟡 High | ✅ Fixed |
| 5 | Two-pass regex scanning | 🟡 High | ✅ Fixed |
| 6 | Regex recompilation | 🟡 High | ✅ Fixed |
| 7 | Poor error tracking | 🟡 Medium | ✅ Fixed |
| 8 | Inefficient persistence | 🟡 Medium | ✅ Fixed |
| 9 | Silent error handling | 🟡 Medium | ✅ Fixed |
| 10 | Inconsistent error messages | 🟢 Low | ✅ Fixed |

### Code Quality Improvements

- ✅ Added comprehensive logging
- ✅ Added module-level documentation
- ✅ Used proper function decomposition
- ✅ Added inline comments for complex logic
- ✅ Consistent error handling
- ✅ Better memory management
- ✅ Type hints in function signatures

---

## Questions?

For questions or issues with the refactored code:
1. Check the troubleshooting section above
2. Review Spark UI for performance insights
3. Check logs for detailed error messages
4. Compare behavior with original code (if still available)

## Conclusion

This refactoring transforms a problematic nested-parallelism approach into a clean, Spark-native architecture that:
- ✅ Fixes all critical bugs
- ✅ Improves performance and stability
- ✅ Reduces memory pressure
- ✅ Enhances error tracking
- ✅ Simplifies maintenance

The explode-based approach is the **right way** to do this in Spark!
