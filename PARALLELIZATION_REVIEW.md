# Image Analysis LLM Parallelization Review

## Executive Summary

This document reviews the transformation from **sequential** to **parallel** LLM calls in the image analysis pipeline, following the proven patterns from the reference table extraction transform.

---

## Current Implementation (Sequential)

### The Bottleneck

In the original code, images are processed **sequentially** within each page:

```python
for img_b64 in image_dict.keys():
    if image_dict[img_b64] is None:
        analysis = get_chart_analysis_fromBase64(img_b64)
        image_dict[img_b64] = analysis
```

### Performance Impact

**Example: Page with 5 images**
- Image 1: 3 seconds
- Image 2: 3 seconds
- Image 3: 3 seconds
- Image 4: 3 seconds
- Image 5: 3 seconds
- **Total: 15 seconds**

With 1000 pages averaging 3 images each: **~2.5 hours** of pure LLM wait time

---

## New Implementation (Parallel)

### Key Changes

#### 1. **Added Concurrency Libraries**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
```

#### 2. **Configuration Constants**
```python
MAX_CONCURRENT_LLM_CALLS = 5  # Adjust based on API rate limits
LLM_TIMEOUT_SECONDS = 120  # Timeout per LLM call
```

#### 3. **LLM Wrapper Function**
Created a wrapper for parallel execution:
```python
def call_llm_for_image(img_b64):
    """
    Wrapper function for calling LLM on a single image.
    This function is submitted to ThreadPoolExecutor.
    Returns tuple of (img_b64, analysis_result)
    """
    try:
        analysis = get_chart_analysis_fromBase64(img_b64)
        return (img_b64, analysis)
    except Exception as e:
        logger.error(f"Error in LLM call for image: {e}")
        return (img_b64, f"Error processing image: {str(e)[:100]}")
```

#### 4. **Parallel Execution in analyse_page()**

**Before (Sequential):**
```python
for img_b64 in image_dict.keys():
    if image_dict[img_b64] is None:
        analysis = get_chart_analysis_fromBase64(img_b64)
        image_dict[img_b64] = analysis
```

**After (Parallel):**
```python
images_to_process = [img_b64 for img_b64, analysis in image_dict.items() if analysis is None]

if images_to_process:
    logger.info(f"Analyzing {len(images_to_process)} unique images in parallel (max_workers={MAX_CONCURRENT_LLM_CALLS})")
    parallel_start = time.time()

    # Use ThreadPoolExecutor for parallel LLM calls
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_LLM_CALLS) as executor:
        # Submit all tasks
        future_to_image = {
            executor.submit(call_llm_for_image, img_b64): img_b64
            for img_b64 in images_to_process
        }

        # Collect results as they complete
        completed_count = 0
        for future in as_completed(future_to_image, timeout=LLM_TIMEOUT_SECONDS * len(images_to_process)):
            try:
                img_b64, analysis = future.result(timeout=LLM_TIMEOUT_SECONDS)
                image_dict[img_b64] = analysis
                completed_count += 1

                if completed_count % 5 == 0 or completed_count == len(images_to_process):
                    logger.info(f"Completed {completed_count}/{len(images_to_process)} image analyses")

            except Exception as e:
                img_b64 = future_to_image[future]
                logger.error(f"Failed to analyze image: {e}")
                image_dict[img_b64] = f"Error processing image: {str(e)[:100]}"

    parallel_duration = time.time() - parallel_start
    logger.info(f"Parallel processing completed {len(images_to_process)} images in {parallel_duration:.2f}s "
               f"(avg {parallel_duration/len(images_to_process):.2f}s per image)")
```

---

## Performance Comparison

### Single Page with 5 Images

| Implementation | Time | Speedup |
|---------------|------|---------|
| Sequential | 15s | 1x |
| Parallel (max_workers=5) | ~3s | **5x faster** |

### Full Dataset (1000 pages, 3 images avg)

| Implementation | Total Time | Speedup |
|---------------|------------|---------|
| Sequential | ~2.5 hours | 1x |
| Parallel (max_workers=5) | ~30 minutes | **5x faster** |

### Spark Cluster Impact

With 16 executors, each processing their partitions in parallel:
- **Sequential**: Each executor blocks on each image
- **Parallel**: Each executor can have 5 concurrent LLM calls = **80 parallel LLM calls cluster-wide**

---

## Key Design Patterns from Reference Code

### 1. **ThreadPoolExecutor Pattern**
```python
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = {
        executor.submit(call_llm_wrapper, llm_name, model, enhanced_prompt, base64_content): llm_name
        for llm_name, model in llm_models.items()
    }

    for future in as_completed(futures):
        llm_name, raw_response = future.result(timeout=90)
        # Process results
```

### 2. **Error Handling Per Future**
Each LLM call has independent error handling:
```python
try:
    img_b64, analysis = future.result(timeout=LLM_TIMEOUT_SECONDS)
    image_dict[img_b64] = analysis
except Exception as e:
    img_b64 = future_to_image[future]
    logger.error(f"Failed to analyze image: {e}")
    image_dict[img_b64] = f"Error processing image: {str(e)[:100]}"
```

### 3. **Progress Tracking**
```python
if completed_count % 5 == 0 or completed_count == len(images_to_process):
    logger.info(f"Completed {completed_count}/{len(images_to_process)} image analyses")
```

### 4. **Performance Metrics**
```python
parallel_duration = time.time() - parallel_start
logger.info(f"Parallel processing completed {len(images_to_process)} images in {parallel_duration:.2f}s "
           f"(avg {parallel_duration/len(images_to_process):.2f}s per image)")
```

---

## Configuration Tuning Guide

### `MAX_CONCURRENT_LLM_CALLS`

| Value | Use Case | Risk |
|-------|----------|------|
| 3 | Conservative, strict API limits | Low risk, moderate speedup |
| 5 | **Recommended** balanced approach | Balanced |
| 10 | Aggressive, high quota | Rate limit risk |

### Calculation for Cluster-Wide Concurrency
```
Total Concurrent Calls = NUM_EXECUTORS × MAX_CONCURRENT_LLM_CALLS
Example: 16 executors × 5 = 80 concurrent calls
```

### Rate Limit Considerations

If you see 429 errors:
1. Reduce `MAX_CONCURRENT_LLM_CALLS` to 3
2. Add backoff in `get_chart_analysis_fromBase64` (already has retry logic)
3. Consider processing in batches if pages have many images

---

## Memory Considerations

### Sequential Approach
- Low memory per executor (processes one image at a time)
- But longer wall-clock time

### Parallel Approach (This Implementation)
- Moderate memory (max 5 images being processed simultaneously per executor)
- Much faster wall-clock time
- **Recommended for most use cases**

### If You Have Pages with 50+ Images

Consider batch processing:
```python
from itertools import islice

def chunked(iterable, size):
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk

# Process in batches of 10
for batch in chunked(images_to_process, 10):
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_LLM_CALLS) as executor:
        # ... process batch
```

---

## Testing Recommendations

### Phase 1: Small Dataset
1. Test with 10-20 pages
2. Use `MAX_CONCURRENT_LLM_CALLS = 3`
3. Monitor for:
   - 429 rate limit errors
   - Timeout issues
   - Memory usage

### Phase 2: Gradual Scale-Up
1. Increase to 100 pages
2. Try `MAX_CONCURRENT_LLM_CALLS = 5`
3. Measure actual speedup
4. Check API quota usage

### Phase 3: Production
1. Run full dataset
2. Monitor cluster metrics
3. Adjust concurrency based on observed performance

---

## Error Handling Improvements

The parallelized version maintains all existing error handling:

1. **Retry logic** in `get_chart_analysis_fromBase64` (5 retries with exponential backoff)
2. **Per-image error handling** in wrapper function
3. **Timeout protection** via `as_completed(timeout=...)`
4. **Graceful degradation** - if one image fails, others continue

---

## Migration Checklist

- [x] Add `concurrent.futures` imports
- [x] Add configuration constants (`MAX_CONCURRENT_LLM_CALLS`, `LLM_TIMEOUT_SECONDS`)
- [x] Create `call_llm_for_image()` wrapper function
- [x] Replace sequential loop with ThreadPoolExecutor pattern
- [x] Add progress logging
- [x] Add performance metrics
- [x] Preserve all existing error handling
- [ ] Test with small dataset (10-20 pages)
- [ ] Monitor API rate limits
- [ ] Measure performance improvement
- [ ] Test with full dataset
- [ ] Tune `MAX_CONCURRENT_LLM_CALLS` based on results

---

## Code Files

- **Original (Sequential)**: Provided in initial request
- **New (Parallel)**: `transforms/image_analysis_transform_parallel.py`
- **Reference Implementation**: Table extraction transform (provided as reference)

---

## Expected Results

### Before
```
Processing 5 images in page
[15 seconds of sequential processing]
Page completed
```

### After
```
Processing 5 images in page
Analyzing 5 unique images in parallel (max_workers=5)
Completed 5/5 image analyses
Parallel processing completed 5 images in 3.2s (avg 0.64s per image)
Page completed
```

---

## Monitoring Metrics

Track these metrics during testing:

1. **Wall-clock time per page**
   - Before: ~(num_images × 3s)
   - After: ~(max(image times))

2. **Throughput (images/minute)**
   - Before: ~20 images/min
   - After: ~100 images/min

3. **API errors**
   - Watch for 429 (rate limit)
   - Watch for timeouts

4. **Memory usage**
   - Should remain stable
   - Each executor processes max 5 images concurrently

---

## Conclusion

The parallelization follows the exact pattern from your reference table extraction code:
- ✅ Uses `ThreadPoolExecutor` for parallel I/O-bound operations
- ✅ Implements proper timeout handling
- ✅ Maintains robust error handling per future
- ✅ Adds comprehensive logging
- ✅ Provides performance metrics

**Estimated speedup: 3-5x faster** depending on `MAX_CONCURRENT_LLM_CALLS` setting.

Start conservative with `MAX_CONCURRENT_LLM_CALLS = 3` and gradually increase based on observed performance and API limits.
