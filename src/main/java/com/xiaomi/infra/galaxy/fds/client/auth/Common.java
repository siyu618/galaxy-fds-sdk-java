package com.xiaomi.infra.galaxy.fds.client.auth;

public class Common {

  public static final String XIAOMI_HEADER_PREFIX = "x-xiaomi-";
  public static final String XIAOMI_META_HEADER_PREFIX =
      XIAOMI_HEADER_PREFIX + "meta-";

  // Required query parameters for pre-signed uri
  public static final String GALAXY_ACCESS_KEY_ID = "GalaxyAccessKeyId";
  public static final String SIGNATURE = "Signature";
  public static final String EXPIRES = "Expires";

  // Http headers used for authentication
  public static final String AUTHORIZATION = "authorization";
  public static final String CONTENT_MD5 = "content-md5";
  public static final String CONTENT_TYPE = "content-type";
  public static final String DATE = "date";
  public static final String TRASH_BUCKET_NAME = "trash";

  // Pre-defined object metadata headers
  public static final String CACHE_CONTROL = "cache-control";
  public static final String CONTENT_ENCODING = "content-encoding";
  public static final String CONTENT_LENGTH = "content-length";
  public static final String LAST_MODIFIED = "last-modified";
  public static final String LAST_CHECKED = "last-checked";
  public static final String UPLOAD_TIME = "upload-time";
  public static final String RANGE = "range";

  // Request properties used for metrics collection
  public static final String ACTION = "action";
  public static final String REQUEST_METRICS = "request-metrics";
  public static final String METRICS_COLLECTOR = "metrics-collector";

  /**
   * The default uri for fds service base uri
   */
  public static final String DEFAULT_FDS_SERVICE_BASE_URI = "http://files.fds.api.xiaomi.com/";

  /**
   * The default uri for cdn service uri
   */
  public static final String DEFAULT_CDN_SERVICE_URI = "http://cdn.fds.api.xiaomi.com/";
}
