import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  
    load_dotenv = None

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / '.env'

if load_dotenv:
    load_dotenv(ENV_PATH)


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == '':
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == '':
        return default
    try:
        return float(value)
    except ValueError:
        return default


GROQ_API_KEY = os.getenv('GROQ_API_KEY', '').strip()
LLM_MODEL = os.getenv('LLM_MODEL', 'llama-3.3-70b-versatile').strip()
LLM_MAX_TOKENS = _get_int('LLM_MAX_TOKENS', 500)
LLM_TEMPERATURE = _get_float('LLM_TEMPERATURE', 0.3)

SIMULATION_TIME = _get_int('SIMULATION_TIME', 60)
COOLDOWN_TICKS = _get_int('COOLDOWN_TICKS', 8)
CACHE_MAX_SIZE = _get_int('CACHE_MAX_SIZE', 200)
THREAD_POOL_WORKERS = _get_int('THREAD_POOL_WORKERS', 4)
OPTIMIZE_EVERY = _get_int('OPTIMIZE_EVERY', 20)
RUNS_PER_SIZE = _get_int('RUNS_PER_SIZE', 5)

ENABLE_PREDICTIVE_ACTIONS = os.getenv('ENABLE_PREDICTIVE_ACTIONS', 'false').lower() == 'true'
ENABLE_OPTIMIZATION = os.getenv('ENABLE_OPTIMIZATION', 'false').lower() == 'true'
ENABLE_FEEDBACK_FOLLOWUPS = os.getenv('ENABLE_FEEDBACK_FOLLOWUPS', 'true').lower() == 'true'


def validate_config() -> list[str]:
    errors = []
    if not GROQ_API_KEY:
        errors.append('Missing GROQ_API_KEY. Add it to .env or your environment.')
    return errors

def get_thread_pool_workers(num_nodes: int) -> int:
    
    if num_nodes <= 10:
        return 4
    elif num_nodes <= 20:
        return 6
    elif num_nodes <= 50:
        return 8
    else:
        return 10
