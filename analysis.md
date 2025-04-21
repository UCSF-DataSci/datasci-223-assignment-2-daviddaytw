### **Analysis Approach**
My approach follows a structured pipeline using **Polars**.

1. **Data Ingestion & Conversion**: Start by reading a large CSV file and converting it to Parquet format.
2. **Lazy Query Execution**: Leveraging **lazy execution** (`scan_parquet()`). This means computations won't be executed immediately, allowing for optimized query planning and efficient memory usage.
3. **Filtering**: Patients with **BMI values outside the range of 10-60** are excluded to maintain relevant cohort analysis.
4. **Feature Selection**: Only **BMI, Glucose, and Age** columns are kept for analysis.
5. **Cohort Categorization**: BMI values are mapped to categories—**Underweight, Normal, Overweight, and Obese**—using `cut()`, helping structure the data into meaningful groups.
6. **Aggregation & Grouping**: Patients are grouped by BMI ranges, and aggregate statistics like **mean glucose levels, patient count, and average age** are calculated efficiently.
7. **Streaming Collection**: Using `collect(streaming=True)`, results are gathered in a streaming fashion, ensuring memory efficiency for large datasets.

---

### **Patterns & Insights**
From the output, we see some interesting trends:
- **Glucose levels increase with BMI**: Patients classified as **Obese** have the highest average glucose level (126.03), while **Underweight** patients show the lowest (95.19). This aligns with known metabolic correlations between BMI and glucose regulation.
- **Patient Distribution**: Most patients fall in the **Obese (3,066,409) and Overweight (1,165,360)** categories, suggesting a prevalence of high BMI in the dataset.
- **Age Differences**: **Underweight patients have the lowest average age (23.98),** while other groups maintain a higher mean age (~32-33). This could suggest that younger individuals might be more likely to be underweight.

---

### **Efficiency Gains with Polars**
- **Lazy Execution:** Delays computation until necessary, reducing memory overhead.
- **Parquet over CSV:** Optimized for large-scale data processing.
- **Vectorized Operations:** No explicit Python loops; operations run in parallel using Rust-based optimizations.
- **Streaming Collection:** Helps efficiently process large datasets without excessive memory use.
