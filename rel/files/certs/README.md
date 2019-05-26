# Certificates directory

By default, this directory should contain WEB key/cert pair and (optionally) a
chain of intermediate CA's for the certificate, under the following names:

* web_cert.pem
* web_key.pem
* web_chain.pem

The default paths can be changed in *app.config* file.

### Certs sharing with Onepanel

If the oz-worker application is started via Onepanel, this directory is NOT 
used, but oz-worker uses the same certificates as Onepanel.

#### NOTE

You should use certificates signed by a trusted CA to ensure secure connections 
for clients.

