package vn.ghtk.connect.replicator.service.http;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class HttpResponseTextHandler implements ResponseHandler<HttpResponseText> {

    @Override
    public HttpResponseText handleResponse(org.apache.http.HttpResponse response) throws ClientProtocolException, IOException {
        HttpEntity entity = response.getEntity();
        HttpResponseText httpResponseText = new HttpResponseText();
        httpResponseText.setCode(response.getStatusLine().getStatusCode());
        httpResponseText.setText(EntityUtils.toString(entity));
        return httpResponseText;
    }

}
