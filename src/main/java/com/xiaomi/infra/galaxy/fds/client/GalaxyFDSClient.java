package com.xiaomi.infra.galaxy.fds.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import javax.net.ssl.SSLContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.LinkedListMultimap;
import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import com.xiaomi.infra.galaxy.fds.client.auth.Common;
import com.xiaomi.infra.galaxy.fds.client.auth.XiaomiHeader;
import com.xiaomi.infra.galaxy.fds.client.auth.signature.Signer;
import com.xiaomi.infra.galaxy.fds.client.auth.signature.Signer.SignAlgorithm;
import com.xiaomi.infra.galaxy.fds.client.bean.BucketBean;
import com.xiaomi.infra.galaxy.fds.client.bean.GrantBean;
import com.xiaomi.infra.galaxy.fds.client.bean.GranteeBean;
import com.xiaomi.infra.galaxy.fds.client.bean.ObjectBean;
import com.xiaomi.infra.galaxy.fds.client.bean.OwnerBean;
import com.xiaomi.infra.galaxy.fds.client.credential.GalaxyFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyException;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.client.filter.FDSClientLogFilter;
import com.xiaomi.infra.galaxy.fds.client.filter.MetricsRequestFilter;
import com.xiaomi.infra.galaxy.fds.client.filter.MetricsResponseFilter;
import com.xiaomi.infra.galaxy.fds.client.metrics.MetricsCollector;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList.Grant;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList.GrantType;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList.Permission;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList.UserGroups;
import com.xiaomi.infra.galaxy.fds.client.metrics.ClientMetrics;
import com.xiaomi.infra.galaxy.fds.client.model.Action;
import com.xiaomi.infra.galaxy.fds.client.model.FDSBucket;
import com.xiaomi.infra.galaxy.fds.client.model.FDSMd5InputStream;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObject;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectInputStream;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectMetadata;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectSummary;
import com.xiaomi.infra.galaxy.fds.client.model.InitMultipartUploadResult;
import com.xiaomi.infra.galaxy.fds.client.model.Owner;
import com.xiaomi.infra.galaxy.fds.client.model.HttpMethod;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlPolicy;
import com.xiaomi.infra.galaxy.fds.client.model.ListAllBucketsResult;
import com.xiaomi.infra.galaxy.fds.client.model.ListDomainMappingsResult;
import com.xiaomi.infra.galaxy.fds.client.model.ListObjectsResult;
import com.xiaomi.infra.galaxy.fds.client.model.PutObjectResult;
import com.xiaomi.infra.galaxy.fds.client.model.QuotaPolicy;
import com.xiaomi.infra.galaxy.fds.client.model.SubResource;
import com.xiaomi.infra.galaxy.fds.client.model.UploadPartResult;
import com.xiaomi.infra.galaxy.fds.client.model.UploadPartResultList;

public class GalaxyFDSClient implements GalaxyFDS {

  private final GalaxyFDSCredential credential;
  private final FDSClientConfiguration fdsConfig;
  private MetricsCollector metricsCollector;
  private String delimiter = "/";
  private final Random random = new Random();
  private final String clientId = UUID.randomUUID().toString().substring(0, 8);
  private HttpClient httpClient;
  private FDSClientLogFilter logFilter = new FDSClientLogFilter();
  private PoolingHttpClientConnectionManager connectionManager;

  public static final int MAX_BATCH_DELETE_SIZE = 1000;

  // TODO(wuzesheng) Make the authenticator configurable and let the
  // authenticator supply sign algorithm and generate signature
  private static SignAlgorithm SIGN_ALGORITHM = SignAlgorithm.HmacSHA1;

  public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
      "EEE, dd MMM yyyy HH:mm:ss z", Locale.US);

  static {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
  }

  private static final Log LOG = LogFactory.getLog(GalaxyFDSClient.class);

  public GalaxyFDSClient(GalaxyFDSCredential credential,
      FDSClientConfiguration fdsConfig) {
    this.credential = credential;
    this.fdsConfig = fdsConfig;

    init();
  }

  private void init() {
    if (fdsConfig.isApacheConnectorEnabled()) {
      LOG.warn("Apache Connector not supported");
    }
    httpClient = createHttpClient(this.fdsConfig);
    if (fdsConfig.isMetricsEnabled()) {
      metricsCollector = new MetricsCollector(this);
    }
  }

  private HttpClient createHttpClient(FDSClientConfiguration config) {
    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(config.getConnectionTimeoutMs())
        .setSocketTimeout(config.getSocketTimeoutMs())
        .build();

    RegistryBuilder<ConnectionSocketFactory> registryBuilder = RegistryBuilder.create();
    registryBuilder.register("http", new PlainConnectionSocketFactory());

    if (config.isHttpsEnabled()) {
      SSLContext sslContext = SSLContexts.createSystemDefault();
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
          sslContext,
          SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
      registryBuilder.register("https", sslsf);
    }
    connectionManager = new PoolingHttpClientConnectionManager(registryBuilder.build());
    connectionManager.setDefaultMaxPerRoute(config.getMaxConnection());
    connectionManager.setMaxTotal(config.getMaxConnection());

    HttpClient httpClient = HttpClients.custom()
        .setConnectionManager(connectionManager)
        .setDefaultRequestConfig(requestConfig)
        .setRetryHandler(new DefaultHttpRequestRetryHandler(3, false))
        .build();
    return httpClient;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  private HttpUriRequest prepareRequestMethod(URI uri,
      HttpMethod method, ContentType contentType, FDSObjectMetadata metadata,
      HashMap<String, String> params, Map<String, List<Object>> headers,
      HttpEntity requestEntity) throws GalaxyFDSClientException {
    if (params != null) {
      URIBuilder builder = new URIBuilder(uri);
      for (Entry<String, String> param : params.entrySet()) {
        builder.addParameter(param.getKey(), param.getValue());
      }
      try {
        uri = builder.build();
      } catch (URISyntaxException e) {
        throw new GalaxyFDSClientException("Invalid param: " + params.toString(), e);
      }
    }

    if (headers == null)
      headers = new HashMap<String, List<Object>>();
    Map<String, Object> h = prepareRequestHeader(uri, method, contentType, metadata);
    for (Entry<String, Object> hIte: h.entrySet()) {
      String key = hIte.getKey();
      if (!headers.containsKey(key)) {
        headers.put(key, new ArrayList<Object>());
      }
      headers.get(key).add(hIte.getValue());
    }

    HttpUriRequest httpRequest;
    switch (method) {
      case PUT:
        HttpPut httpPut = new HttpPut(uri);
        if (requestEntity != null)
          httpPut.setEntity(requestEntity);
        httpRequest = httpPut;
        break;
      case GET:
        httpRequest = new HttpGet(uri);
        break;
      case DELETE:
        httpRequest = new HttpDelete(uri);
        break;
      case HEAD:
        httpRequest = new HttpHead(uri);
        break;
      case POST:
        HttpPost httpPost = new HttpPost(uri);
        if (requestEntity != null)
          httpPost.setEntity(requestEntity);
        httpRequest = httpPost;
        break;
      default:
        throw new GalaxyFDSClientException("Method " + method.name() +
            " not supported");
    }
    for (Entry<String, List<Object>> header: headers.entrySet()) {
      String key = header.getKey();
      if (key == null || key.isEmpty())
        continue;

      for (Object obj: header.getValue()) {
        if (obj == null)
          continue;
        httpRequest.addHeader(header.getKey(), obj.toString());
      }
    }

    return httpRequest;
  }

  @Override
  public List<FDSBucket> listBuckets() throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), "", (SubResource[]) null);

    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, null, null, null);
    HttpResponse response = executeHttpRequest(httpRequest, Action.ListBuckets);

    ListAllBucketsResult result = (ListAllBucketsResult)processResponse(response, ListAllBucketsResult.class,
        "list buckets");

    ArrayList<FDSBucket> buckets = new ArrayList<FDSBucket>();
    if (result != null) {
      OwnerBean owner = result.getOwner();
      for (BucketBean b : result.getBuckets()) {
        FDSBucket bucket = new FDSBucket(b.getName());
        bucket.setOwner(new Owner(owner.getId(), owner.getDisplayName()));
        buckets.add(bucket);
      }
    }
    return buckets;
  }

  private <T> Object processResponse(HttpResponse response, Class<T> c,
      String purposeStr) throws GalaxyFDSClientException {
    HttpEntity httpEntity = response.getEntity();
    int statusCode = response.getStatusLine().getStatusCode();
    try {
      if (statusCode == HttpStatus.SC_OK) {
        if (c != null) {
          Gson gson = new Gson();
          Reader reader = new InputStreamReader(httpEntity.getContent());
          T entityVal = gson.fromJson(reader, c);
          return entityVal;
        }
        return null;
      } else {
        String errorMsg = formatErrorMsg(purposeStr, response);
        LOG.error(errorMsg);
        throw new GalaxyFDSClientException(errorMsg);
      }
    } catch (IOException e) {
      String errorMsg = formatErrorMsg("read response entity", e);
      LOG.error(errorMsg);
      throw new GalaxyFDSClientException(errorMsg, e);
    } finally {
      closeResponseEntity(response);
    }
  }

  private HttpResponse executeHttpRequest(HttpUriRequest httpRequest,
      Action action) throws GalaxyFDSClientException {

    HttpContext context = null;
    if (fdsConfig.isMetricsEnabled()) {
      context = new BasicHttpContext();
      context.setAttribute(Common.ACTION, httpRequest);
      context.setAttribute(Common.ACTION, action);
      context.setAttribute(Common.METRICS_COLLECTOR, metricsCollector);
      MetricsRequestFilter requestFilter = new MetricsRequestFilter();
      try {
        requestFilter.filter(context);
      } catch (IOException e) {
        LOG.error("fail to call request filter", e);
      }
    }

    HttpResponse response = null;
    try {
      try {
        response = httpClient.execute(httpRequest);
      } catch (IOException e) {
        LOG.error("http request failed", e);
        throw new GalaxyFDSClientException(e.getMessage(), e);
      }

      return response;
    } finally {
      if (fdsConfig.isMetricsEnabled()) {
        try {
          logFilter.filter(httpRequest, response);
        } catch (IOException e) {
          LOG.error("log filter failed", e);
        }
        MetricsResponseFilter responseFilter = new MetricsResponseFilter();
        try {
          responseFilter.filter(context);
        } catch (IOException e) {
          LOG.error("fail to call response filter", e);
        }
      }
    }
  }

  @Override
  public void createBucket(String bucketName) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    StringEntity requestEntity = getJsonStringEntity("{}", ContentType.APPLICATION_JSON);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        ContentType.APPLICATION_JSON, null, null, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.PutBucket);

    try {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        String errorMsg = formatErrorMsg("create bucket [" + bucketName + "]", response);
        LOG.error(errorMsg);
        throw new GalaxyFDSClientException(errorMsg);
      }
    } finally {
      closeResponseEntity(response);
    }
  }

  private void closeResponseEntity(HttpResponse response) {
    if (response == null)
      return;
    HttpEntity entity = response.getEntity();
    if (entity != null && entity.isStreaming())
      try {
        entity.getContent().close();
      } catch (IOException e) {
        LOG.error(formatErrorMsg("close response entity", e));
      }
  }

  @Override
  public void deleteBucket(String bucketName) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);

    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.DELETE, null, null, null, null, null);
    HttpResponse response = executeHttpRequest(httpRequest, Action.PutBucket);

    processResponse(response, null, "delete bucket [" + bucketName + "]");
  }

  @Override
  public void getBucket(String bucketName) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.GetBucketMeta);

    processResponse(response, null, "get bucket [" + bucketName + "]");
  }

  @Override
  public boolean doesBucketExist(String bucketName)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.HEAD, null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.HeadBucket);

    int statusCode = response.getStatusLine().getStatusCode();
    try {
      if (statusCode == HttpStatus.SC_OK)
        return true;
      else if (statusCode == HttpStatus.SC_NOT_FOUND)
        return false;
      else {
        String errorMsg = formatErrorMsg("check bucket [" + bucketName + "] existence", response);
        LOG.error(errorMsg);
        throw new GalaxyFDSClientException(errorMsg);
      }
    } finally {
      closeResponseEntity(response);
    }
  }

  private String getResponseEntityPhrase(HttpResponse response) {
    try {
      InputStream inputStream = response.getEntity().getContent();
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      byte[] data = new byte[1024];
      for (int count; (count = inputStream.read(data, 0, 1024)) != -1;)
        outputStream.write(data, 0, count);
      String reason = outputStream.toString();
      if (reason == null || reason.isEmpty())
        return response.getStatusLine().getReasonPhrase();
      return reason;
    } catch (Exception e) {
      LOG.error("Fail to get entity string");
      return response.getStatusLine().getReasonPhrase();
    }
  }

  @Override
  public AccessControlList getBucketAcl(String bucketName)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, SubResource.ACL);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.GetBucketACL);

    AccessControlPolicy acp = (AccessControlPolicy) processResponse(response,
        AccessControlPolicy.class, "get bucket [" + bucketName + "] acl");
    return acpToAcl(acp);
  }

  @Override
  public void setBucketAcl(String bucketName, AccessControlList acl)
      throws GalaxyFDSClientException {
    Preconditions.checkNotNull(acl);

    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, SubResource.ACL);
    ContentType contentType = ContentType.APPLICATION_JSON;
    AccessControlPolicy acp = aclToAcp(acl);
    StringEntity requestEntity = getJsonStringEntity(acp, contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT, contentType, null, null, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.PutBucketACL);

    processResponse(response, null, "set bucket [" + bucketName + "] acl");
  }

  @Override
  public QuotaPolicy getBucketQuota(String bucketName)
      throws GalaxyFDSClientException {
    Preconditions.checkNotNull(bucketName);

    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, SubResource.QUOTA);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.GetBucketQuota);

    QuotaPolicy quotaPolicy = (QuotaPolicy)processResponse(response,
        QuotaPolicy.class, "get bucket [" + bucketName + "] quota");
    return quotaPolicy;
  }

  @Override
  public void setBucketQuota(String bucketName, QuotaPolicy quotaPolicy)
      throws GalaxyFDSClientException {
    Preconditions.checkNotNull(quotaPolicy);
    Preconditions.checkNotNull(bucketName);

    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, SubResource.QUOTA);
    ContentType contentType = ContentType.APPLICATION_JSON;
    HttpEntity requestEntity = getJsonStringEntity(quotaPolicy, contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT, contentType, null, null, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.PutBucketQuota);

    processResponse(response, null, "set bucket [" + bucketName + "] quota");
  }

  @Override
  public FDSObjectListing listObjects(String bucketName)
      throws GalaxyFDSClientException {
    return listObjects(bucketName, "", this.delimiter);
  }

  @Override
  public FDSObjectListing listObjects(String bucketName, String prefix)
      throws GalaxyFDSClientException {
    return listObjects(bucketName, prefix, this.delimiter);
  }

  @Override
  public FDSObjectListing listObjects(String bucketName, String prefix,
      String delimiter) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("prefix", prefix);
    params.put("delimiter", delimiter);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, params, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.ListObjects);

    ListObjectsResult listObjectsResult = (ListObjectsResult)processResponse(response, ListObjectsResult.class, "list objects under bucket [" + bucketName + "] with prefix [" + prefix + "]");
    return getObjectListing(listObjectsResult);
  }

  @Override
  public FDSObjectListing listTrashObjects(String prefix, String delimiter)
      throws GalaxyFDSClientException {
    return listObjects(Common.TRASH_BUCKET_NAME, prefix, delimiter);
  }

  @Override
  public FDSObjectListing listNextBatchOfObjects(
      FDSObjectListing previousObjectListing) throws GalaxyFDSClientException {
    if (!previousObjectListing.isTruncated()) {
      LOG.warn("The previous listObjects() response is complete, " +
          "call of listNextBatchOfObjects() will be ingored");
      return null;
    }

    String bucketName = previousObjectListing.getBucketName();
    String prefix = previousObjectListing.getPrefix();
    String delimiter = previousObjectListing.getDelimiter();
    String marker = previousObjectListing.getNextMarker();
    int maxKeys = previousObjectListing.getMaxKeys();
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("prefix", prefix);
    params.put("delimiter", delimiter);
    params.put("marker", marker);
    params.put("maxKeys", Integer.toString(maxKeys));
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, params, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.ListObjects);

    ListObjectsResult listObjectsResult = (ListObjectsResult)processResponse(response,
        ListObjectsResult.class,
        "list next batch of objects under bucket [" + bucketName + "]" +
            " with prefix [" + prefix + "], marker [" + marker + "]");
    return getObjectListing(listObjectsResult);
  }

  @Override
  public PutObjectResult putObject(String bucketName, String objectName,
      File file) throws GalaxyFDSClientException {
    FileInputStream stream = null;
    try {
      stream = new FileInputStream(file);
      return putObject(bucketName, objectName, stream, file.length(), null);
    } catch (FileNotFoundException e) {
      String errorMsg = "File not found, file=" + file.getName();
      LOG.error(errorMsg);
      throw new GalaxyFDSClientException(errorMsg, e);
    } finally {
      closeInputStream(stream);
    }
  }

  private PutObjectResult putObject(String bucketName, String objectName,
      InputStream input, long contentLength, FDSObjectMetadata metadata)
      throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
    if (metadata != null && metadata.getContentType() != null) {
      contentType = ContentType.create(metadata.getContentType());
    }
    if (fdsConfig.isMd5CalculateEnabled()) {
      if (metadata == null) {
        metadata = new FDSObjectMetadata();
      }
      metadata.addHeader(XiaomiHeader.MD5_ATTACHED_STREAM.getName(), "1");
      try {
        input = new FDSMd5InputStream(input);
      } catch (NoSuchAlgorithmException e) {
        throw new GalaxyFDSClientException("Cannot init md5", e);
      }
    }
    URI uri = formatUri(fdsConfig.getUploadBaseUri(), bucketName + "/"
        + objectName, (SubResource[]) null);
    InputStreamEntity requestEntity = getInputStreamRequestEntity(input, contentType);

    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.PUT, contentType, metadata, null, null, requestEntity);
    if (contentLength >= 0)
      httpRequest.setHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(contentLength));

    HttpResponse response = executeHttpRequest(httpRequest, Action.PutObject);

    PutObjectResult putObjectResult = (PutObjectResult)processResponse(response,
        PutObjectResult.class,
        "put object [" + objectName + "] to bucket [" + bucketName + "]");
    return putObjectResult;
  }

  @Override
  public PutObjectResult putObject(String bucketName, String objectName,
      InputStream input, FDSObjectMetadata metadata)
      throws GalaxyFDSClientException {
    return putObject(bucketName, objectName, input, -1, metadata);
  }

  private PutObjectResult postObject(String bucketName, InputStream input,
      long contentLen, FDSObjectMetadata metadata) throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
    if (metadata != null && metadata.getContentType() != null) {
      contentType = ContentType.create(metadata.getContentType());
    }
    if (fdsConfig.isMd5CalculateEnabled()) {
      if (metadata == null) {
        metadata = new FDSObjectMetadata();
      }
      metadata.addHeader(XiaomiHeader.MD5_ATTACHED_STREAM.getName(), "1");
      try {
        input = new FDSMd5InputStream(input);
      } catch (NoSuchAlgorithmException e) {
        throw new GalaxyFDSClientException("Cannot init md5", e);
      }
    }
    URI uri = formatUri(fdsConfig.getUploadBaseUri(), bucketName + "/",
        (SubResource[]) null);
    InputStreamEntity requestEntity = getInputStreamRequestEntity(input, contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.POST, contentType, metadata, null, null, requestEntity);
    if (contentLen >= 0)
      httpRequest.setHeader(HttpHeaders.CONTENT_LENGTH, String.valueOf(contentLen));

    HttpResponse response = executeHttpRequest(httpRequest, Action.PostObject);

    PutObjectResult putObjectResult = (PutObjectResult)processResponse(response,
        PutObjectResult.class,
        "post object to bucket [" + bucketName + "]");
    return putObjectResult;
  }

  @Override
  public PutObjectResult postObject(String bucketName, File file)
      throws GalaxyFDSClientException {
    FileInputStream stream = null;
    try {
      stream = new FileInputStream(file);
      return postObject(bucketName, stream, file.length(), null);
    } catch (FileNotFoundException e) {
      String errorMsg = "File not found, file=" + file.getName();
      LOG.error(errorMsg);
      throw new GalaxyFDSClientException(errorMsg, e);
    } finally {
      closeInputStream(stream);
    }
  }

  @Override
  public PutObjectResult postObject(String bucketName, InputStream input,
      FDSObjectMetadata metadata) throws GalaxyFDSClientException {
    return postObject(bucketName, input, -1, metadata);
  }

  @Override
  public FDSObject getObject(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    // start from position 0 by default
    return getObject(bucketName, objectName, 0);
  }

  @Override
  public FDSObject getObject(String bucketName, String objectName, long pos)
      throws GalaxyFDSClientException {
    if (pos < 0) {
      String errorMsg = "get object " + objectName + " from bucket "
          + bucketName + " failed, reason=invalid seek position:" + pos;
      LOG.error(errorMsg);
      throw new GalaxyFDSClientException(errorMsg);
    }
    URI uri = formatUri(fdsConfig.getDownloadBaseUri(), bucketName + "/"
        + objectName, (SubResource[]) null);
    Map<String, List<Object>> headers = new HashMap<String, List<Object>>();
    if (pos > 0) {
      List<Object> objects = new ArrayList<Object>();
      objects.add("bytes=" + pos + "-");
      headers.put(Common.RANGE, objects);
    }
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, null, headers, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.GetObject);

    HttpEntity httpEntity = response.getEntity();
    FDSObject rtnObject = null;
    try {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_PARTIAL_CONTENT) {
        FDSObjectSummary summary = new FDSObjectSummary();
        summary.setBucketName(bucketName);
        summary.setObjectName(objectName);
        summary.setSize(httpEntity.getContentLength());

        FDSObjectInputStream stream = new FDSObjectInputStream(httpEntity);
        rtnObject = new FDSObject();
        rtnObject.setObjectSummary(summary);
        rtnObject.setObjectContent(stream);
        rtnObject.setObjectMetadata(FDSObjectMetadata.parseObjectMetadata(
            response.getAllHeaders()));

        return rtnObject;
      } else {
        String errorMsg = formatErrorMsg("get object [" + objectName + "] from bucket [" + bucketName + "]", response);
        LOG.error(errorMsg);
        throw new GalaxyFDSClientException(errorMsg);
      }
    } catch (IOException e) {
      String errorMsg = formatErrorMsg("read entity stream", e);
      LOG.error(errorMsg);
      throw new GalaxyFDSClientException(errorMsg, e);
    } finally {
      if (rtnObject == null) {
        closeResponseEntity(response);
      }
    }
  }

  @Override
  public FDSObjectMetadata getObjectMetadata(String bucketName,
      String objectName) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        SubResource.METADATA);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.GetObjectMetadata);

    try {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == HttpStatus.SC_OK) {
        FDSObjectMetadata metadata = FDSObjectMetadata.parseObjectMetadata(
            response.getAllHeaders());
        return metadata;
      } else {
        String errorMsg = formatErrorMsg("get metadata for object [" + objectName +
            "] under bucket [" + bucketName + "]", response);
        LOG.error(errorMsg);
        throw new GalaxyFDSClientException(errorMsg);
      }
    } finally {
      closeResponseEntity(response);
    }
  }

  @Override
  public AccessControlList getObjectAcl(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        SubResource.ACL);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.GET, null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.GetObjectACL);

    AccessControlPolicy acp = (AccessControlPolicy)processResponse(response,
        AccessControlPolicy.class,
        "get acl for object [" + objectName + "] under bucket [" + bucketName + "]");
    return acpToAcl(acp);
  }

  @Override
  public void setObjectAcl(String bucketName, String objectName,
      AccessControlList acl) throws GalaxyFDSClientException {
    Preconditions.checkNotNull(acl);
    AccessControlPolicy acp = aclToAcp(acl);
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        SubResource.ACL);
    StringEntity requestEntity = getJsonStringEntity(acp, ContentType.APPLICATION_JSON);
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.PUT,
        ContentType.APPLICATION_JSON, null, null, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.PutObjectACL);

    processResponse(response, null, "set acl for object [" +
        objectName + "] under bucket [" + bucketName + "]");
  }

  @Override
  public void deleteObjectAcl(String bucketName, String objectName,
      AccessControlList acl) throws GalaxyFDSClientException {
    Preconditions.checkNotNull(acl);
    AccessControlPolicy acp = aclToAcp(acl);
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        SubResource.ACL);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("action", "delete");
    ContentType contentType = ContentType.APPLICATION_JSON;
    StringEntity requestEntity = getJsonStringEntity(acp, ContentType.APPLICATION_JSON);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.DeleteObjectACL);

    processResponse(response, null, "delete acl for object [" + objectName + "] under bucket ["
        + bucketName + "]");
  }

  @Override
  public boolean doesObjectExist(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        (SubResource[]) null);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.HEAD,
        null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.HeadObject);

    int statusCode = response.getStatusLine().getStatusCode();
    try {
      if (statusCode == HttpStatus.SC_OK)
        return true;
      else if (statusCode == HttpStatus.SC_NOT_FOUND)
        return false;
      else {
        String errorMsg = formatErrorMsg("check existence of object [" + objectName +
            "] under bucket [" + bucketName + "]", response);
        LOG.error(errorMsg);
        throw new GalaxyFDSClientException(errorMsg);
      }
    } finally {
      closeResponseEntity(response);
    }
  }

  @Override
  public void deleteObject(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        (SubResource[]) null);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.DELETE,
        null, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.DeleteObject);

    processResponse(response, null, "delete object [" + objectName + "] under bucket ["
        + bucketName + "]");
  }

  @Override
  public List<Map<String, Object>> deleteObjects(String bucketName, String prefix)
      throws GalaxyFDSClientException {
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(prefix);

    List<Map<String, Object> > resultList = new ArrayList<Map<String, Object>>();

    FDSObjectListing objects = listObjects(bucketName, prefix, "");

    long totalItemsListed = 0, totalItemsDeleted = 0;
    for (int iterationCnt = 0; objects != null; ++iterationCnt) {
      int itemsLeft = objects.getObjectSummaries().size();

      totalItemsListed += itemsLeft;
      List<String> objectNameList = new ArrayList<String>();

      totalItemsDeleted += itemsLeft;
      if (itemsLeft > 0)
        for (FDSObjectSummary s : objects.getObjectSummaries()) {
          String objectName = s.getObjectName();
          objectNameList.add(objectName);
          --itemsLeft;
          if (objectNameList.size() >= MAX_BATCH_DELETE_SIZE || itemsLeft <= 0) {
            try {
              List<Map<String, Object>> errorList = deleteObjects(bucketName, objectNameList);
              totalItemsDeleted -= errorList.size();
              resultList.addAll(errorList);
            } catch (Exception e) {
              LOG.warn("fail to delete objects", e);
              // retry with small batch size
              try {
                Thread.sleep(500);
              } catch (InterruptedException e1) {
              }
              for (int index = 0, interval = Math.max(10, objectNameList.size() / 10);
                   index < objectNameList.size(); ) {
                int to = Math.min(index + interval, objectNameList.size());
                resultList.addAll(deleteObjects(bucketName,
                    objectNameList.subList(index, to)));
                index = to;
              }
            }
            objectNameList.clear();
          }
        }

      LOG.info("" + iterationCnt + "th round, "
          + " total items listed: " + totalItemsListed
          + " total items deleted: " + totalItemsDeleted
          + " total errors: " + resultList.size());

      objects = listNextBatchOfObjects(objects);
    }

    return resultList;
  }

  @Override
  public List<Map<String, Object>> deleteObjects(String bucketName,
      List<String> objectNameList)
      throws GalaxyFDSClientException {
    Preconditions.checkNotNull(bucketName);
    Preconditions.checkNotNull(objectNameList);

    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    ContentType contentType = ContentType.APPLICATION_JSON;
    StringEntity requestEntity = getJsonStringEntity(objectNameList, contentType);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("deleteObjects", "");
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.DeleteObjects);

    List<Map<String, Object>> responseList = (List<Map<String, Object>>)processResponse(
        response, List.class,
        "delete " + objectNameList.size() + " objects under bucket [" + bucketName + "]");
    return responseList;
  }

  @Override
  public void restoreObject(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_JSON;
    StringEntity requestEntity = getJsonStringEntity("", contentType);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("restore", "");
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        (SubResource[]) null);
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.PUT, contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.RestoreObject);

    processResponse(response, null,
        "restore object [" + objectName + "] under bucket ["
        + bucketName + "]");
  }

  @Override
  public void renameObject(String bucketName, String srcObjectName,
      String dstObjectName) throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + srcObjectName,
        (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("renameTo", dstObjectName);
    StringEntity requestEntity = getJsonStringEntity("", contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.PUT, contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.RenameObject);

    processResponse(response, null, "rename object [" + srcObjectName +
        "] to object [" + dstObjectName + "] under bucket [" + bucketName + "]");
  }

  @Override
  public void prefetchObject(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_JSON;
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("prefetch", "");
    StringEntity requestEntity = getJsonStringEntity("", contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.PrefetchObject);

    processResponse(response, null, "prefetch object [" + objectName + "] under bucket [" +
        bucketName + "]");
  }

  @Override
  public void refreshObject(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_JSON;
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("refresh", "");
    StringEntity requestEntity = getJsonStringEntity("", contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.RefreshObject);

    processResponse(response, null, "refresh object [" + objectName + "] under bucket [" +
        bucketName + "]");
  }

  @Override
  public void putDomainMapping(String bucketName, String domainName)
      throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_JSON;
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName,
        (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("domain", domainName);
    StringEntity requestEntity = getJsonStringEntity("", contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.PutDomainMapping);

    processResponse(response, null, "add domain mapping; bucket [" + bucketName
        + "], domainName [" + domainName + "]");
  }

  @Override
  public List<String> listDomainMappings(String bucketName)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("domain", "");
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT, null, null, params, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.ListDomainMappings);

    ListDomainMappingsResult result = (ListDomainMappingsResult)processResponse(response,
        ListDomainMappingsResult.class,
        "list domain mappings; bucket [" + bucketName + "]");
    return result.getDomainMappings();
  }

  @Override
  public void deleteDomainMapping(String bucketName, String domainName)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName, (SubResource[]) null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("domain", domainName);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.DELETE,
        null, null, params, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.DeleteDomainMapping);

    processResponse(response, null, "delete domain mapping; bucket [" + bucketName
        + "], domain [" + domainName + "]");
  }

  public void cropImage(String bucketName, String objectName,
      int x, int y, int w, int h)
      throws GalaxyFDSClientException {
    ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        (SubResource[]) null);

    HashMap<String, String> params = new HashMap<String, String>();
    params.put("cropImage", "");
    params.put("x", Integer.toString(x));
    params.put("y", Integer.toString(y));
    params.put("w", Integer.toString(w));
    params.put("h", Integer.toString(h));
    StringEntity requestEntity = getJsonStringEntity("", contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.CropImage);

    processResponse(response, null, "crop image; bucket [" + bucketName
        + "], object [" + objectName + "]");
  }

  @Override
  public void setPublic(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    setPublic(bucketName, objectName, false);
  }

  @Override
  public void setPublic(String bucketName, String objectName,
      boolean disablePrefetch) throws GalaxyFDSClientException {
    AccessControlList acl = new AccessControlList();
    acl.addGrant(new Grant(UserGroups.ALL_USERS.name(), Permission.READ,
        GrantType.GROUP));
    setObjectAcl(bucketName, objectName, acl);
    if (!disablePrefetch) {
      prefetchObject(bucketName, objectName);
    }
  }

  @Override
  public URI generateDownloadObjectUri(String bucketName, String objectName)
      throws GalaxyFDSClientException {
    return formatUri(fdsConfig.getDownloadBaseUri(), bucketName + "/"
        + objectName, (SubResource[]) null);
  }

  @Override
  public URI generatePresignedUri(String bucketName, String objectName,
      Date expiration) throws GalaxyFDSClientException {
    return generatePresignedUri(bucketName, objectName,
        expiration, HttpMethod.GET);
  }

  @Override
  public URI generatePresignedCdnUri(String bucketName, String objectName,
      Date expiration) throws GalaxyFDSClientException {
    return generatePresignedCdnUri(bucketName, objectName,
        expiration, HttpMethod.GET);
  }

  @Override
  public URI generatePresignedUri(String bucketName, String objectName,
      Date expiration, HttpMethod httpMethod) throws GalaxyFDSClientException {
    try {
      return Signer.generatePresignedUri(fdsConfig.getBaseUri(), bucketName, objectName,
          null, expiration, httpMethod, credential.getGalaxyAccessId(),
          credential.getGalaxyAccessSecret(), SIGN_ALGORITHM);
    } catch (GalaxyException e) {
      throw new GalaxyFDSClientException(e);
    }
  }

  @Override
  public URI generatePresignedCdnUri(String bucketName, String objectName,
      Date expiration, HttpMethod httpMethod) throws GalaxyFDSClientException {
    try {
      return Signer.generatePresignedUri(fdsConfig.getCdnBaseUri(), bucketName,
          objectName, null, expiration, httpMethod, credential.getGalaxyAccessId(),
          credential.getGalaxyAccessSecret(), SIGN_ALGORITHM);
    } catch (GalaxyException e) {
      throw new GalaxyFDSClientException(e);
    }
  }

  @Override
  public URI generatePresignedUri(String bucketName, String objectName,
      SubResource subResource, Date expiration, HttpMethod httpMethod)
      throws GalaxyFDSClientException {
    List<String> subResources = new ArrayList<String>();
    subResources.add(subResource.getName());
    return generatePresignedUri(bucketName, objectName, subResources,
        expiration, httpMethod);
  }

  @Override
  public URI generatePresignedUri(String bucketName, String objectName,
      List<String> subResources, Date expiration, HttpMethod httpMethod)
      throws GalaxyFDSClientException {
    try {
      return Signer.generatePresignedUri(fdsConfig.getBaseUri(), bucketName, objectName,
          subResources, expiration, httpMethod, credential.getGalaxyAccessId(),
          credential.getGalaxyAccessSecret(), SIGN_ALGORITHM);
    } catch (GalaxyException e) {
      throw new GalaxyFDSClientException(e);
    }
  }

  @Override
  public URI generatePresignedCdnUri(String bucketName, String objectName,
      SubResource subResource, Date expiration, HttpMethod httpMethod)
      throws GalaxyFDSClientException {
    List<String> subResources = new ArrayList<String>();
    subResources.add(subResource.getName());
    return generatePresignedCdnUri(bucketName, objectName, subResources,
        expiration, httpMethod);
  }

  @Override
  public URI generatePresignedCdnUri(String bucketName, String objectName,
      List<String> subResources, Date expiration, HttpMethod httpMethod)
      throws GalaxyFDSClientException {
    try {
      return Signer.generatePresignedUri(fdsConfig.getCdnBaseUri(), bucketName,
          objectName, subResources, expiration, httpMethod,
          credential.getGalaxyAccessId(), credential.getGalaxyAccessSecret(),
          SIGN_ALGORITHM);
    } catch (GalaxyException e) {
      throw new GalaxyFDSClientException(e);
    }
  }

  @Override
  public InitMultipartUploadResult initMultipartUpload(String bucketName,
      String objectName) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        SubResource.UPLOADS);
    ContentType contentType = ContentType.APPLICATION_JSON;
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.PUT, contentType, null, null, null, null);

    HttpResponse response = executeHttpRequest(httpRequest, Action.InitMultiPartUpload);

    InitMultipartUploadResult initMultipartUploadResult = (InitMultipartUploadResult)processResponse(response,
        InitMultipartUploadResult.class,
        "init multipart upload object [" + objectName +
            "] to bucket [" + bucketName + "]");
    return initMultipartUploadResult;
  }

  @Override
  public UploadPartResult uploadPart(String bucketName, String objectName,
      String uploadId, int partNumber, InputStream in)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        null);
    ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("uploadId", uploadId);
    params.put("partNumber", String.valueOf(partNumber));
    InputStreamEntity requestEntity = getInputStreamRequestEntity(in, contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.PUT, contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.UploadPart);

    UploadPartResult uploadPartResult = (UploadPartResult)processResponse(response,
        UploadPartResult.class,
        "upload part of object [" + objectName +
            "] to bucket [" + bucketName + "]" + "; part number [" +
            partNumber + "], upload id [" + uploadId + "]" );

    return uploadPartResult;
  }

  @Override
  public PutObjectResult completeMultipartUpload(String bucketName,
      String objectName, String uploadId, FDSObjectMetadata metadata,
      UploadPartResultList uploadPartResultList) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        null);
    ContentType contentType = ContentType.APPLICATION_JSON;
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("uploadId", uploadId);
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.PUT, contentType, null, params, null, null);

    HttpResponse response = executeHttpRequest(httpRequest,
        Action.CompleteMultiPartUpload);

    PutObjectResult putObjectResult = (PutObjectResult)processResponse(response,
        PutObjectResult.class,
        "complete multipart upload of object [" + objectName +
            "] to bucket [" + bucketName + "]" + "; upload id [" + uploadId + "]" );
    return putObjectResult;
  }

  @Override
  public void abortMultipartUpload(String bucketName, String objectName,
      String uploadId) throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), bucketName + "/" + objectName,
        null);
    ContentType contentType = ContentType.APPLICATION_JSON;
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("uploadId", uploadId);
    HttpUriRequest httpRequest = prepareRequestMethod(uri,
        HttpMethod.DELETE, contentType, null, params, null, null);

    HttpResponse response = executeHttpRequest(httpRequest,
        Action.AbortMultiPartUpload);

    processResponse(response, null,
        "abort multipart upload of object [" + objectName +
            "] to bucket [" + bucketName + "]" +
            "; upload id [" + uploadId + "]" );
  }

  /**
   * Put client metrics to server. This method should only be used internally.
   *
   * @param clientMetrics Metrics to be pushed to server.
   * @throws GalaxyFDSClientException
   */
  public void putClientMetrics(ClientMetrics clientMetrics)
      throws GalaxyFDSClientException {
    URI uri = formatUri(fdsConfig.getBaseUri(), "", (SubResource[]) null);
    ContentType contentType = ContentType.APPLICATION_JSON;
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("clientMetrics", "");
    HttpEntity requestEntity = getJsonStringEntity(clientMetrics, contentType);
    HttpUriRequest httpRequest = prepareRequestMethod(uri, HttpMethod.PUT,
        contentType, null, params, null, requestEntity);

    HttpResponse response = executeHttpRequest(httpRequest, Action.PutClientMetrics);

    processResponse(response, null, "put client metrics");
  }

  private StringEntity getJsonStringEntity(Object entityContent, ContentType mediaType) {
    Gson gson = new Gson();
    String jsonStr = gson.toJson(entityContent);
    StringEntity entity = new StringEntity(jsonStr, mediaType);
    return entity;
  }

  private InputStreamEntity getInputStreamRequestEntity(InputStream input,
      ContentType contentType) {
    input = new BufferedInputStream(input);
    InputStreamEntity entity = new InputStreamEntity(input, contentType);
    return entity;
  }

  URI formatUri(String baseUri,
      String resource, SubResource... subResourceParams)
      throws GalaxyFDSClientException {
    String subResource = null;
    if (subResourceParams != null) {
      for (SubResource param : subResourceParams) {
        if (subResource != null) {
          subResource += "&" + param.getName();
        } else {
          subResource = param.getName();
        }
      }
    }

    try {
      URI uri = new URI(baseUri);
      String schema = uri.getScheme();
      String host = uri.getHost();
      int port = uri.getPort();
      URI encodedUri;
      if (subResource == null) {
        encodedUri = new URI(schema, null, host, port, "/" + resource,
            null, null);
      } else {
        encodedUri = new URI(schema, null, host, port, "/" + resource,
            subResource, null);
      }
      return encodedUri;
    } catch (URISyntaxException e) {
      LOG.error("Invalid uri syntax", e);
      throw new GalaxyFDSClientException("Invalid uri syntax", e);
    }
  }

  Map<String, Object> prepareRequestHeader(URI uri,
      HttpMethod method, ContentType contentType, FDSObjectMetadata metadata)
      throws GalaxyFDSClientException {
    LinkedListMultimap<String, String> headers = LinkedListMultimap.create();

    if (metadata != null) {
      for (Map.Entry<String, String> e : metadata.getRawMetadata().entrySet()) {
        headers.put(e.getKey(), e.getValue());
      }
    }

    // Format date
    String date = DATE_FORMAT.format(new Date());
    headers.put(Common.DATE, date);

    // Set content type
    if (contentType != null)
      headers.put(Common.CONTENT_TYPE, contentType.toString());

    // Set unique request id
    headers.put(XiaomiHeader.REQUEST_ID.getName(), getUniqueRequestId());

    // Set authorization information
    byte[] signature;
    try {
      URI relativeUri = new URI(uri.toString().substring(
          uri.toString().indexOf('/', uri.toString().indexOf(':') + 3)));
      signature = Signer.signToBase64(method, relativeUri, headers,
          credential.getGalaxyAccessSecret(), SIGN_ALGORITHM);
    } catch (InvalidKeyException e) {
      LOG.error("Invalid secret key spec", e);
      throw new GalaxyFDSClientException("Invalid secret key sepc", e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Unsupported signature algorithm:" + SIGN_ALGORITHM, e);
      throw new GalaxyFDSClientException("Unsupported signature slgorithm:"
          + SIGN_ALGORITHM, e);
    } catch (Exception e) {
      throw new GalaxyFDSClientException(e);
    }
    String authString = "Galaxy-V2 " + credential.getGalaxyAccessId() + ":"
        + new String(signature);
    headers.put(Common.AUTHORIZATION, authString);

    Map<String, Object> httpHeaders =
        new HashMap<String, Object>();
    for (Entry<String, String> entry : headers.entries()) {
      httpHeaders.put(entry.getKey(), entry.getValue());
    }
    return httpHeaders;
  }

  AccessControlList acpToAcl(AccessControlPolicy acp) {
    AccessControlList acl = null;
    if (acp != null) {
      acl = new AccessControlList();
      for (GrantBean g : acp.getAccessControlList()) {
        acl.addGrant(new Grant(g.getGrantee().getId(),
            g.getPermission(), g.getType()));
      }
    }
    return acl;
  }

  AccessControlPolicy aclToAcp(AccessControlList acl) {
    AccessControlPolicy acp = null;
    if (acl != null) {
      acp = new AccessControlPolicy();
      acp.setOwner(new OwnerBean(credential.getGalaxyAccessId()));
      List<GrantBean> grants = new ArrayList<GrantBean>(
          acl.getGrantList().size());
      for (Grant g : acl.getGrantList()) {
        grants.add(new GrantBean(new GranteeBean(g.getGranteeId()),
            g.getPermission(), g.getType()));
      }
      acp.setAccessControlList(grants);
    }
    return acp;
  }

  FDSObjectListing getObjectListing(ListObjectsResult result) {
    FDSObjectListing listing = null;
    if (result != null) {
      listing = new FDSObjectListing();
      listing.setBucketName(result.getName());
      listing.setPrefix(result.getPrefix());
      listing.setDelimiter(result.getDelimiter());
      listing.setMarker(result.getMarker());
      listing.setNextMarker(result.getNextMarker());
      listing.setMaxKeys(result.getMaxKeys());
      listing.setTruncated(result.isTruncated());

      List<FDSObjectSummary> summaries = new ArrayList<FDSObjectSummary>(
          result.getObjects().size());
      for (ObjectBean o : result.getObjects()) {
        FDSObjectSummary summary = new FDSObjectSummary();
        summary.setBucketName(result.getName());
        summary.setObjectName(o.getName());
        summary.setSize(o.getSize());
        summary.setOwner(new Owner(o.getOwner().getId(),
            o.getOwner().getDisplayName()));
        summaries.add(summary);
      }
      listing.setObjectSummaries(summaries);
      listing.setCommonPrefixes(result.getCommonPrefixes());
    }
    return listing;
  }

  private String getUniqueRequestId() {
    return clientId + "_" + random.nextInt();
  }

  private String formatErrorMsg(String purpose, Exception e) {
    String msg = "failed to " + purpose + ", " + e.getMessage();
    return msg;
  }

  private String formatErrorMsg(String purpose, HttpResponse response) {
    String msg = "failed to " + purpose + ", status=" +
        response.getStatusLine().getStatusCode() +
        ", reason=" + getResponseEntityPhrase(response);
    return msg;
  }

  void closeInputStream(InputStream inputStream) throws GalaxyFDSClientException {
    if (inputStream != null) {
      try {
        inputStream.close();
      } catch (IOException e) {
        String errorMsg = "close file input stream failed";
        LOG.error(errorMsg);
        throw new GalaxyFDSClientException(errorMsg, e);
      }
    }
  }
}
