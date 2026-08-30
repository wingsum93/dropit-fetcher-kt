package com.ericho.dropit

open class AppException(message: String) : RuntimeException(message)

class TokenExpiredException(message: String) : AppException(message)

class RateLimitException(message: String) : AppException(message)

class DataInvalidateException(message: String) : AppException(message)
