package vn.ghtk.connect.replicator.service.http;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpUriRequest;

import java.io.IOException;

public interface HttpService {

    HttpResponseText execute(HttpUriRequest request) throws IOException;

    HttpResponseText get(String url, Header[] headers) throws IOException;

    HttpResponseText post(String url, Header[] headers, String body) throws IOException;

}
