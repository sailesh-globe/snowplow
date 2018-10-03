## Remote Adapter Example

Snowplow has a large number of webhooks that let you ingest events from various third-party service providers:
see https://github.com/snowplow/snowplow/wiki/Setting-up-a-webhook

If you would like to sponsor a new webhook integration for use by the Snowplow community, please check that page.

But if you need to ingest events from a source where such a public community-wide integration would be inappropriate or impossible, you can implement your own custom integration using Akka remoting, via an Enrich Remote Adapter. This directory contains a simplified instance of such an adapter.

When run, this simple app will be reachable using Akka at the following uri: akka.tcp://remoteTestSystem@127.0.0.1:8995/user/testActor

You can then configure Enrich to talk to this app whenever it receives notification content at a given url. Simply create a config file to define how Enrich's Akka remoting should work, and to define the remote adapters you want to enable, and tell Enrich about this config file when you start it up.
 
Here is an example config file which will cause Enrich to call this simple remote adapter whenever content is posted to (http://your-collector-url/com.example/v1) :
```
akka {
    actor {
        provider:remote
    }
    remote {
      enabled-transports = ["akka.remote.netty.tcp"]
      netty.tcp {
        hostname = 127.0.0.1
        port = 8809
      }
    }
}

remoteAdapters:[
    {
        vendor: "com.example", 
        version: "v1", 
        url: "akka.tcp://remoteTestSystem@127.0.0.1:8995/user/testActor", 
        timeout: 1000
    }
]
```

To tell Enrich where to find this config file, add something like the following to your Enrich startup command line:
    `-DremoteAdapterConfig=myRemoteAdapter.config`
