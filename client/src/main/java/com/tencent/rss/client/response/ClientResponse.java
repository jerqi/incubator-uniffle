package com.tencent.rss.client.response;

public class ClientResponse {

  private ResponseStatusCode statusCode;

  public ClientResponse(ResponseStatusCode statusCode) {
    this.statusCode = statusCode;
  }

  public ResponseStatusCode getStatusCode() {
    return statusCode;
  }
}
