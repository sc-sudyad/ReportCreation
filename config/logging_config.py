import logging
import logging.config


def setup_logging(log_file='app.log'):
    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
        },
        'handlers': {
            'file': {
                'level': 'INFO',
                'formatter': 'standard',
                'class': 'logging.FileHandler',
                'filename': log_file,
            },
            'console': {
                'level': 'INFO',
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
            },
        },
        'root': {
            'handlers': ['file', 'console'],
            'level': 'INFO',
        },
        'loggers': {
            '__main__': {
                'handlers': ['file', 'console'],
                'level': 'INFO',
                'propagate': False,
            },
        }
    }

    logging.config.dictConfig(logging_config)


# Initialize logging configuration
setup_logging()
