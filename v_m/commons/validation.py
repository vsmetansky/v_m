class ValidationException(Exception):
    pass


def validate_date_range(start, stop):
    if start > stop:
        raise ValidationException()
