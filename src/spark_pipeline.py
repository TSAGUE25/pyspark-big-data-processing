"""
PySpark log processing pipeline — with pandas fallback for environments without Spark.
The Spark version demonstrates the same transformations using the real PySpark API.
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


# ── PySpark implementation ───────────────────────────────────────────────────

def create_spark_session(app_name='LogPipeline'):
    if not SPARK_AVAILABLE:
        raise ImportError('PySpark not installed. Use pandas fallback.')
    return (SparkSession.builder
            .appName(app_name)
            .master('local[*]')
            .config('spark.driver.memory', '2g')
            .getOrCreate())


def run_spark_pipeline(csv_path):
    """Full Spark pipeline: ingest → clean → aggregate → window → output."""
    spark = create_spark_session()

    # 1. Ingest
    df = (spark.read
          .option('header', 'true')
          .option('inferSchema', 'true')
          .csv(str(csv_path)))

    # 2. Type cast + filter
    df = (df.withColumn('timestamp', F.to_timestamp('timestamp'))
            .withColumn('hour', F.hour('timestamp'))
            .withColumn('date',  F.to_date('timestamp'))
            .filter(F.col('response_ms') < 30000))

    # 3. Aggregations
    hourly_errors = (df.groupBy('date', 'hour', 'service')
                       .agg(F.count('*').alias('nb_requests'),
                            F.sum('is_error').alias('nb_errors'),
                            F.avg('response_ms').alias('avg_resp_ms'),
                            F.max('response_ms').alias('max_resp_ms'))
                       .withColumn('error_rate',
                                   F.round(F.col('nb_errors') / F.col('nb_requests') * 100, 2)))

    # 4. Window function: cumulative errors per service per day
    w = Window.partitionBy('service', 'date').orderBy('hour').rowsBetween(Window.unboundedPreceding, 0)
    hourly_errors = hourly_errors.withColumn('cumul_errors', F.sum('nb_errors').over(w))

    # 5. Top slow services
    slow = (df.groupBy('service')
              .agg(F.percentile_approx('response_ms', 0.95).alias('p95_ms'),
                   F.percentile_approx('response_ms', 0.99).alias('p99_ms'))
              .orderBy(F.desc('p95_ms')))

    hourly_pd = hourly_errors.toPandas()
    slow_pd   = slow.toPandas()
    spark.stop()
    return hourly_pd, slow_pd


# ── Pandas fallback (same logic) ─────────────────────────────────────────────

def run_pandas_pipeline(df):
    """Equivalent pipeline using pandas when Spark is not available."""
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['hour'] = df['timestamp'].dt.hour
    df['date'] = df['timestamp'].dt.date
    df = df[df['response_ms'] < 30000]

    hourly = (df.groupby(['date', 'hour', 'service'])
                .agg(nb_requests=('is_error', 'count'),
                     nb_errors=('is_error', 'sum'),
                     avg_resp_ms=('response_ms', 'mean'),
                     max_resp_ms=('response_ms', 'max'))
                .reset_index())
    hourly['error_rate'] = (hourly['nb_errors'] / hourly['nb_requests'] * 100).round(2)
    hourly['cumul_errors'] = (hourly.sort_values('hour')
                                     .groupby(['service', 'date'])['nb_errors']
                                     .cumsum()
                                     .values)

    slow = (df.groupby('service')['response_ms']
              .quantile([0.95, 0.99]).unstack()
              .rename(columns={0.95: 'p95_ms', 0.99: 'p99_ms'})
              .reset_index()
              .sort_values('p95_ms', ascending=False))

    return hourly, slow


def log_volume_by_level(df):
    return df.groupby(['service', 'log_level']).size().unstack(fill_value=0)


def error_rate_by_region(df):
    return (df.groupby('region')
              .agg(error_rate=('is_error', 'mean'),
                   avg_response=('response_ms', 'mean'),
                   nb_requests=('is_error', 'count'))
              .round(4))


def plot_spark_results(df, hourly, slow):
    fig, axes = plt.subplots(2, 3, figsize=(16, 9))

    # 1. Log levels distribution
    levels = df['log_level'].value_counts()
    axes[0, 0].bar(levels.index, levels.values,
                   color=['#4CAF50', '#2196F3', '#FF9800', '#F44336', '#9C27B0'],
                   edgecolor='white')
    axes[0, 0].set_title('Volume par niveau de log')

    # 2. Error rate by service
    err_svc = (df.groupby('service')['is_error'].mean() * 100).sort_values(ascending=False)
    colors  = ['#F44336' if e > 10 else '#FF9800' if e > 5 else '#4CAF50'
               for e in err_svc.values]
    axes[0, 1].bar(err_svc.index, err_svc.values, color=colors, edgecolor='white')
    axes[0, 1].tick_params(axis='x', rotation=45)
    axes[0, 1].set_title('Taux d\'erreur par service (%)')
    axes[0, 1].axhline(10, color='red', ls='--', lw=1.5)

    # 3. Response time distribution (log scale)
    axes[0, 2].hist(np.log10(df['response_ms'].clip(1)), bins=50, color='#5C6BC0', edgecolor='white')
    axes[0, 2].set_xlabel('log10(response_ms)'); axes[0, 2].set_title('Distribution temps réponse')

    # 4. Hourly traffic
    hourly_total = hourly.groupby('hour')['nb_requests'].sum()
    axes[1, 0].bar(hourly_total.index, hourly_total.values, color='#2196F3', alpha=0.8)
    axes[1, 0].set_title('Trafic par heure'); axes[1, 0].set_xlabel('Heure')

    # 5. P95 latency by service
    axes[1, 1].barh(slow['service'], slow['p95_ms'], color='#FF9800', edgecolor='white', label='P95')
    axes[1, 1].barh(slow['service'], slow['p99_ms'], color='#F44336', edgecolor='white',
                    alpha=0.5, label='P99')
    axes[1, 1].set_title('Latence P95/P99 par service (ms)'); axes[1, 1].legend()

    # 6. Status code distribution
    sc = df['status_code'].value_counts().head(10)
    colors_sc = ['#4CAF50' if c < 400 else '#FF9800' if c < 500 else '#F44336'
                 for c in sc.index]
    axes[1, 2].bar(sc.index.astype(str), sc.values, color=colors_sc, edgecolor='white')
    axes[1, 2].set_title('Distribution codes HTTP')

    title = 'PySpark Log Processing Pipeline'
    if SPARK_AVAILABLE:
        title += ' (Spark)'
    else:
        title += ' (Pandas fallback — même logique)'
    plt.suptitle(title, fontweight='bold', fontsize=13)
    plt.tight_layout(); plt.show()
