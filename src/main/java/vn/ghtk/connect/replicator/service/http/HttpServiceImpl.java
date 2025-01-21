package vn.ghtk.connect.replicator.service.http;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

public class HttpServiceImpl implements HttpService {

    private PoolingHttpClientConnectionManager httpClientPool = new PoolingHttpClientConnectionManager();
    private ConnectionKeepAliveStrategy keepAliveStrategy;
    private CloseableHttpClient httpClient;

    public HttpServiceImpl() {
        httpClientPool.setMaxTotal(10_000);
        httpClientPool.setDefaultMaxPerRoute(10_000);
        keepAliveStrategy = (HttpResponse hr, HttpContext hc) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator(hr.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 100 * 1000;
        };
        httpClient = HttpClients
                .custom()
                .setKeepAliveStrategy(keepAliveStrategy)
                .setConnectionManager(httpClientPool)
                .build();
    }

    @Override
    public HttpResponseText execute(HttpUriRequest request) throws IOException {
        ResponseHandler<HttpResponseText> responseHandler = new HttpResponseTextHandler();
        HttpResponseText response = httpClient.execute(request, responseHandler);
        return response;
    }

    @Override
    public HttpResponseText get(String url, Header[] headers) throws IOException {
        HttpGet httpGet = new HttpGet(url);
        try {
            httpGet.setConfig(getRequestConfig());
            if (ArrayUtils.isNotEmpty(headers)) {
                httpGet.setHeaders(headers);
            }
            ResponseHandler<HttpResponseText> responseHandler = new HttpResponseTextHandler();
            HttpResponseText httpData = httpClient.execute(httpGet, responseHandler);
            return httpData;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
        }
    }

    @Override
    public HttpResponseText post(String url, Header[] headers, String body) throws IOException {
        HttpPost httpPost = new HttpPost(url);
        try {
            httpPost.setConfig(getRequestConfig());
            if (ArrayUtils.isNotEmpty(headers)) {
                httpPost.setHeaders(headers);
            }
            if (StringUtils.isNotEmpty(body)) {
                StringEntity stringEntity = new StringEntity(body, HTTP.UTF_8);
                httpPost.setEntity(stringEntity);
            }

            ResponseHandler<HttpResponseText> responseHandler = new HttpResponseTextHandler();
            HttpResponseText httpData = httpClient.execute(httpPost, responseHandler);
            return httpData;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
        }
    }

    private RequestConfig getRequestConfig() {
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(10_000)
                .setConnectTimeout(10_000)
                .setConnectionRequestTimeout(10_000)
                .build();
        return requestConfig;
    }
}
