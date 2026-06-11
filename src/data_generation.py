import numpy as np
import pandas as pd
from pathlib import Path


def generate_log_data(n=100_000, seed=42):
    """Generate simulated server event log data for Spark processing demo."""
    rng = np.random.default_rng(seed)

    services   = ['api-gateway', 'auth-service', 'billing', 'catalog',
                  'user-service', 'notification', 'analytics']
    log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL']
    regions    = ['eu-west-1', 'eu-central-1', 'us-east-1', 'ap-southeast-1']
    status_codes = [200, 200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 500, 503]

    timestamps = pd.date_range('2024-01-01', periods=n, freq='3s')

    return pd.DataFrame({
        'timestamp':   timestamps,
        'service':     rng.choice(services, n, p=[0.25, 0.15, 0.10, 0.20, 0.15, 0.08, 0.07]),
        'log_level':   rng.choice(log_levels, n, p=[0.10, 0.60, 0.18, 0.10, 0.02]),
        'region':      rng.choice(regions, n, p=[0.40, 0.25, 0.25, 0.10]),
        'status_code': rng.choice(status_codes, n),
        'response_ms': np.clip(rng.lognormal(4.5, 1.0, n), 1, 30000).round(0).astype(int),
        'user_id':     rng.integers(1, 50001, n),
        'session_id':  rng.integers(1, 200001, n),
        'bytes_sent':  rng.integers(100, 500000, n),
        'is_error':    rng.choice([0, 0, 0, 0, 0, 0, 0, 0, 0, 1], n),
    })


def load_or_generate(csv_path, n=100_000, seed=42):
    path = Path(csv_path)
    if path.exists():
        return pd.read_csv(path, parse_dates=['timestamp'])
    df = generate_log_data(n=n, seed=seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return df
