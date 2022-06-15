import os

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s"
        },
        "json": {
            "class": "pythonjsonlogger.jsonlogger.JsonFormatter",
            "format": "%(asctime)s %(name)s %(processName)s %(threadName)s %(levelname)s %(message)s %(filename)s"
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "class": "logging.StreamHandler",
            "formatter": "json"
        },
        "file": {
            "level": "ERROR",
            "class": "mrfh.MultiprocessRotatingFileHandler",
            "filename": "./logs/main.log",
            "maxBytes": 10000000,
            "backupCount": 3,
            "encoding": "utf-8",
            "formatter": "json"
        }
    },
    "loggers": {
        "": {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False
        }
    }
}
